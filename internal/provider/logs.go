package provider

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	protocol "github.com/colony-2/pulse-docker/internal/engine"
	"github.com/colony-2/pulse-docker/internal/launchlogs"
	api "github.com/colony-2/pulse-provider-api"
)

type LoggingConfig = launchlogs.Config
type Options struct{ Logging *LoggingConfig }
type captureGroup struct {
	store  *launchlogs.Store
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	active map[string]*captureState
	wg     sync.WaitGroup
}

func NewWithOptions(ctx context.Context, cfg Config, o Options) (*Provider, error) {
	if o.Logging == nil {
		return New(ctx, cfg)
	}
	logging, err := o.Logging.Normalize()
	if err != nil {
		return nil, err
	}
	p, err := New(ctx, cfg)
	if err != nil {
		return nil, err
	}
	store, err := launchlogs.Open(logging, p.native.daemonID)
	if err != nil {
		p.Close()
		return nil, err
	}
	life, cancel := context.WithCancel(context.Background())
	p.capture = &captureGroup{store: store, ctx: life, cancel: cancel, active: map[string]*captureState{}}
	p.logs = launchlogs.New(store.GetLaunchLog)
	p.native.capture = p.capture
	// Inventory recovery is observational. It never reconstructs a submission.
	_, found, err := p.native.usage(ctx)
	if err != nil {
		p.Close()
		return nil, err
	}
	history, err := store.Histories()
	if err != nil {
		p.Close()
		return nil, err
	}
	for _, h := range history {
		if h.Capture == "closed" || h.Capture == "expired" {
			continue
		}
		c, ok := found[h.LaunchID]
		if !ok {
			p.capture.warn(store.Finish(h.LaunchID, true, true))
			continue
		}
		p.capture.warn(store.Finish(h.LaunchID, false, true))
		p.capture.start(p.native.e, h.LaunchID, c.ID, true)
	}
	return p, nil
}
func (p *Provider) GetLaunchLog(ctx context.Context, q api.LaunchLogRequest) (api.LaunchLogResponse, error) {
	if err := p.enter(ctx); err != nil {
		return api.LaunchLogResponse{}, err
	}
	defer p.leave()
	if p.capture == nil {
		return api.LaunchLogResponse{}, api.ErrLaunchLogUnsupported
	}
	return p.capture.store.GetLaunchLog(ctx, q)
}
func (c *captureGroup) warn(err error) {
	if err != nil {
		slog.Warn("Docker log observation unavailable")
	}
}
func (c *captureGroup) close() { c.cancel(); c.wg.Wait() }
func (c *captureGroup) start(e *protocol.Engine, id, container string, recovery bool) {
	c.mu.Lock()
	if c.active[id] != nil || c.ctx.Err() != nil {
		c.mu.Unlock()
		return
	}
	if len(c.active) >= 128 {
		c.mu.Unlock()
		c.warn(c.store.Finish(id, true, true))
		return
	}
	state := &captureState{admitted: make(chan struct{})}
	if recovery {
		state.once.Do(func() { close(state.admitted) })
	}
	c.active[id] = state
	c.wg.Add(1)
	c.mu.Unlock()
	// This handshake completes before the caller starts the worker. The stream's
	// lifetime is independent of the Submit deadline and listener lifetimes.
	attachCtx, stop := context.WithCancel(c.ctx)
	timeout := time.AfterFunc(5*time.Second, stop)
	var r io.ReadCloser
	var err error
	if recovery {
		r, err = e.Logs(attachCtx, container)
	} else {
		r, err = e.AttachOutput(attachCtx, container)
	}
	timeout.Stop()
	if err != nil {
		stop()
		c.mu.Lock()
		delete(c.active, id)
		c.mu.Unlock()
		c.wg.Done()
		c.warn(c.store.Finish(id, true, true))
		return
	}
	go func() {
		defer c.wg.Done()
		defer stop()
		defer r.Close()
		defer func() { c.mu.Lock(); delete(c.active, id); c.mu.Unlock() }()
		type fragment struct {
			source api.LogSource
			data   []byte
			native time.Time
		}
		queue := make(chan fragment, 64)
		readDone := make(chan error, 1)
		// Compare the previously committed native prefix on restart. If rotation or
		// interleaving makes it unverifiable, stop explicitly rather than duplicate it.
		var prefix []api.LaunchLogRecord
		var token string
		prefixIndex := 0
		prefixOffset := 0
		prefixFinished := !recovery
		var nativeTimestamp time.Time
		emit := func(src api.LogSource, data []byte) error {
			for !prefixFinished {
				if prefixIndex == len(prefix) {
					p, e := c.store.GetLaunchLog(attachCtx, api.LaunchLogRequest{LaunchID: id, PageToken: token})
					if e != nil {
						return e
					}
					prefix = prefix[:0]
					for _, record := range p.Records {
						if record.Source == api.LogSourceStdout || record.Source == api.LogSourceStderr {
							prefix = append(prefix, record)
						}
					}
					prefixIndex = 0
					token = p.NextPageToken
					if len(prefix) == 0 {
						if p.HasMore {
							continue
						}
						prefixFinished = true
						break
					}
				}
				if prefixIndex < len(prefix) {
					old := prefix[prefixIndex]
					n := min(len(data), len(old.Data)-prefixOffset)
					if src != old.Source || !bytes.Equal(data[:n], old.Data[prefixOffset:prefixOffset+n]) {
						return errors.New("Docker replay boundary cannot be verified")
					}
					data = data[n:]
					prefixOffset += n
					if prefixOffset == len(old.Data) {
						prefixIndex++
						prefixOffset = 0
					}
					if len(data) == 0 {
						return nil
					}
				}
			}
			if len(data) == 0 {
				return nil
			}
			select {
			case queue <- fragment{src, append([]byte(nil), data...), nativeTimestamp}:
				return nil
			case <-attachCtx.Done():
				return attachCtx.Err()
			default:
				return api.ErrLogCapacity
			}
		}
		go func() {
			var err error
			if recovery {
				err = protocol.ReadTimestampedOutput(r, func(src api.LogSource, ts time.Time, data []byte) error { nativeTimestamp = ts; return emit(src, data) })
			} else {
				err = protocol.ReadOutput(r, emit)
			}
			close(queue)
			readDone <- err
		}()
		storageFailed := false
		for f := range queue {
			if !storageFailed {
				if err = c.store.AppendNative(id, f.source, f.data, f.native); err != nil {
					storageFailed = true
					r.Close()
				}
			}
		}
		readErr := <-readDone
		select {
		case <-state.admitted:
		case <-c.ctx.Done():
		}
		interrupted := c.ctx.Err() != nil
		incomplete := recovery || storageFailed || readErr != nil
		c.warn(c.store.Finish(id, !interrupted, incomplete))
	}()
}

type captureState struct {
	admitted chan struct{}
	once     sync.Once
}

func (c *captureGroup) admitted(id string) {
	c.mu.Lock()
	state := c.active[id]
	c.mu.Unlock()
	if state != nil {
		state.once.Do(func() { close(state.admitted) })
	}
}
