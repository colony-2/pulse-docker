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
	store       *launchlogs.Store
	ctx         context.Context
	cancel      context.CancelFunc
	scope       string
	now         func() time.Time
	waitTimeout time.Duration
	mu          sync.Mutex
	active      map[string]*captureState
	wg          sync.WaitGroup
}

func NewWithOptions(ctx context.Context, cfg Config, o Options) (*Provider, error) {
	return newWithOptions(ctx, cfg, o, time.Now)
}

func newWithOptions(ctx context.Context, cfg Config, o Options, now func() time.Time) (*Provider, error) {
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
	p.capture = &captureGroup{store: store, ctx: life, cancel: cancel, scope: retentionScope(logging, p.native.daemonID), now: now, active: map[string]*captureState{}}
	p.logs = launchlogs.New(store.GetLaunchLog)
	p.native.capture = p.capture
	p.capture.reconcile(ctx, p.native, true)
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
func (c *captureGroup) start(p *engineProvider, id, container string, recovery bool, stopped bool) {
	c.mu.Lock()
	if c.active[id] != nil || c.ctx.Err() != nil {
		c.mu.Unlock()
		return
	}
	if len(c.active) >= 128 {
		c.mu.Unlock()
		c.mark(id, container, "capture_capacity")
		return
	}
	state := &captureState{admitted: make(chan struct{}), started: make(chan bool, 1)}
	if recovery {
		state.once.Do(func() { close(state.admitted) })
		state.started <- false
	}
	c.active[id] = state
	c.wg.Add(1)
	c.mu.Unlock()
	attachCtx, stop := context.WithCancel(c.ctx)
	var r io.ReadCloser
	if !recovery {
		timeout := time.AfterFunc(5*time.Second, stop)
		var err error
		r, err = p.e.AttachOutput(attachCtx, container)
		timeout.Stop()
		if err != nil {
			c.mark(id, container, "attach_failed")
		}
	}
	go func() {
		defer c.wg.Done()
		defer stop()
		defer func() { c.mu.Lock(); delete(c.active, id); c.mu.Unlock() }()
		result := make(chan string, 1)
		if recovery {
			c.mark(id, container, "provider_interrupted")
		}
		if recovery && !stopped {
			go func() {
				reason, _ := c.replay(attachCtx, p.e, id, container, true)
				if reason != "" && attachCtx.Err() == nil {
					c.recoveryFailed(id, container, reason)
				}
				result <- reason
			}()
		} else if r != nil {
			go func() {
				defer r.Close()
				reason, _ := c.read(attachCtx, id, r, false)
				if reason == "" {
					timer := time.NewTimer(250 * time.Millisecond)
					defer timer.Stop()
					select {
					case <-timer.C:
						if worker, err := inspectWorker(attachCtx, p.e, container); err == nil && (worker.State.Status == "running" || worker.State.Status == "paused") {
							reason = "output_ended"
						}
					case <-attachCtx.Done():
					}
				}
				if reason != "" && attachCtx.Err() == nil {
					c.mark(id, container, reason)
				}
				result <- reason
			}()
		} else {
			result <- "replay_needed"
		}
		var started bool
		select {
		case started = <-state.started:
		case <-c.ctx.Done():
		}
		outcome := c.observe(p.e, id, container, started)
		var reason string
		select {
		case reason = <-result:
		case <-c.ctx.Done():
			stop()
			<-result
		case <-time.After(5 * time.Second):
			stop()
			<-result
			reason = "drain_timeout"
		}
		if c.ctx.Err() != nil {
			c.mark(id, container, "provider_interrupted")
			c.warn(c.store.Finish(id, false, true))
			return
		}
		if reason != "" && reason != "replay_needed" {
			c.mark(id, container, reason)
		}
		if outcome.missing {
			c.mark(id, container, "container_missing")
		}
		if !outcome.stopped && !outcome.missing && !outcome.confirmed {
			return
		}
		expired := false
		if recovery && outcome.stopped {
			if obs, err := c.store.Observation(id); err == nil && obs.Receipt != nil {
				anchor := obs.Receipt.StoppedAt
				if process := obs.History.Process; process != nil && process.FinishedAt != nil {
					anchor = process.FinishedAt
				}
				expired = c.graceExpired(anchor)
			}
		}
		deferFinalization := false
		if !expired && !outcome.missing && reason != "replay_unverifiable" {
			why, suffix := c.replay(c.ctx, p.e, id, container, false)
			if why == "replay_unverifiable" && reason == "" && !recovery && c.equivalentRetainedOutput(p.e, id, container) {
				why = ""
			}
			if reason == "" && suffix && !recovery {
				c.mark(id, container, "output_ended")
			}
			if why != "" {
				c.recoveryFailed(id, container, why)
				switch why {
				case "archive_budget", "queue_capacity", "storage_unavailable", "native_logs_unavailable", "output_read_failed":
					deferFinalization = true
				}
			}
		}
		select {
		case <-state.admitted:
		case <-c.ctx.Done():
			return
		}
		if outcome.saved && !expired && !deferFinalization {
			c.report(id, container, "terminal_commit", c.store.Complete(id))
		}
		if outcome.stopped || outcome.missing {
			c.cleanup(p, id, container)
		}
	}()
}

var errReplay = errors.New("Docker replay boundary cannot be verified")

func (c *captureGroup) read(attachCtx context.Context, id string, r io.ReadCloser, recovery bool) (string, bool) {
	return c.readWithAppend(attachCtx, id, r, recovery, c.store.AppendNative)
}

func (c *captureGroup) readWithAppend(attachCtx context.Context, id string, r io.ReadCloser, recovery bool, appendOutput func(string, api.LogSource, []byte, time.Time) error) (string, bool) {
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
					return errReplay
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
		if recovery && err == nil && !prefixFinished {
			err = finishPrefix(attachCtx, c.store, id, token, prefixIndex == len(prefix), prefix != nil)
		}
		close(queue)
		readDone <- err
	}()
	var storageErr error
	var appended bool
	for f := range queue {
		if storageErr == nil {
			if storageErr = appendOutput(id, f.source, f.data, f.native); storageErr != nil {
				r.Close()
			} else {
				appended = true
			}
		}
	}
	readErr := <-readDone
	switch {
	case errors.Is(storageErr, launchlogs.ErrBudget):
		return "archive_budget", appended
	case storageErr != nil:
		return "storage_unavailable", appended
	case errors.Is(readErr, api.ErrLogCapacity):
		return "queue_capacity", appended
	case errors.Is(readErr, errReplay):
		return "replay_unverifiable", appended
	case readErr != nil:
		return "output_read_failed", appended
	default:
		return "", appended
	}
}

func finishPrefix(ctx context.Context, store *launchlogs.Store, id, token string, consumed, loaded bool) error {
	if !consumed {
		return errReplay
	}
	if !loaded {
		token = ""
	}
	for {
		page, err := store.GetLaunchLog(ctx, api.LaunchLogRequest{LaunchID: id, PageToken: token})
		if err != nil {
			return err
		}
		for _, record := range page.Records {
			if record.Source != api.LogSourceLaunch {
				return errReplay
			}
		}
		if !page.HasMore {
			return nil
		}
		token = page.NextPageToken
	}
}

func (c *captureGroup) replay(ctx context.Context, e *engine, id, container string, follow bool) (string, bool) {
	if err := c.store.ResetAppend(id); err != nil {
		if errors.Is(err, launchlogs.ErrBudget) {
			return "archive_budget", false
		}
		return "storage_unavailable", false
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	timeout := time.AfterFunc(5*time.Second, cancel)
	defer timeout.Stop()
	r, err := e.ReadLogs(ctx, container, follow)
	if err != nil {
		return "native_logs_unavailable", false
	}
	if follow {
		timeout.Stop()
	}
	defer r.Close()
	return c.read(ctx, id, r, true)
}

type captureState struct {
	admitted chan struct{}
	started  chan bool
	once     sync.Once
}

func (c *captureGroup) started(id string, started bool) {
	c.mu.Lock()
	state := c.active[id]
	c.mu.Unlock()
	if state != nil {
		select {
		case state.started <- started:
		default:
		}
	}
}

func (c *captureGroup) admitted(id string) {
	c.mu.Lock()
	state := c.active[id]
	c.mu.Unlock()
	if state != nil {
		state.once.Do(func() { close(state.admitted) })
	}
}
