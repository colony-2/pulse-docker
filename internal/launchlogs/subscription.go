package launchlogs

import (
	"context"
	"errors"
	"fmt"
	api "github.com/colony-2/pulse-provider-api"
	"sync"
	"time"
)

type Reader func(context.Context, api.LaunchLogRequest) (api.LaunchLogResponse, error)
type Manager struct {
	mu                            sync.Mutex
	closed                        bool
	resident                      int
	subs                          map[*subscription]bool
	Read                          Reader
	CallbackTimeout, PollInterval time.Duration
}

func New(read Reader) *Manager {
	return &Manager{Read: read, subs: map[*subscription]bool{}, CallbackTimeout: 5 * time.Second, PollInterval: 50 * time.Millisecond}
}
func Validate(o api.SubmitOptions) error {
	if o.Listener != nil && (o.Listener.Context == nil || o.Listener.OnLog == nil || o.Listener.Duration < 0) {
		return fmt.Errorf("%w: listener needs context, callback and nonnegative duration", api.ErrLogProtocol)
	}
	return nil
}

type subscription struct {
	mu            sync.Mutex
	manager       *Manager
	ctx           context.Context
	cancel        context.CancelFunc
	done          chan struct{}
	result        api.ListenerResult
	coverage      []api.SourceStatus
	options       api.ListenOptions
	registered    time.Time
	ids           []string
	admitted      map[string]api.Status
	pending       map[string]int
	admissionDone bool
	ended         bool
}

func (m *Manager) Subscribe(ls []api.Launch, o api.SubmitOptions) api.LogSubscription {
	if o.Listener == nil {
		return nil
	}
	ctx, cancel := context.WithCancel(o.Listener.Context)
	s := &subscription{manager: m, ctx: ctx, cancel: cancel, done: make(chan struct{}), options: *o.Listener, registered: time.Now(), admitted: map[string]api.Status{}, pending: map[string]int{}}
	for _, l := range ls {
		s.ids = append(s.ids, l.LaunchID)
		for _, src := range []api.LogSource{api.LogSourceLaunch, api.LogSourceStdout, api.LogSourceStderr} {
			s.coverage = append(s.coverage, api.SourceStatus{LaunchID: l.LaunchID, Source: src, State: api.SourcePending})
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		s.finish(api.ListenerProviderClosed, api.ErrProviderClosed)
		return s
	}
	if m.Read == nil {
		s.unavailable()
		return s
	}
	if m.resident >= 32 {
		s.finish(api.ListenerCapacityExceeded, api.ErrLogCapacity)
		return s
	}
	m.resident++
	m.subs[s] = true
	go s.run()
	return s
}
func (m *Manager) Admitted(sub api.LogSubscription, rs []api.Submission) {
	s, ok := sub.(*subscription)
	if !ok {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range rs {
		s.admitted[r.LaunchID] = r.Status
	}
	s.admissionDone = true
}
func (m *Manager) Close() {
	m.mu.Lock()
	m.closed = true
	subs := make([]*subscription, 0, len(m.subs))
	for s := range m.subs {
		subs = append(subs, s)
	}
	m.mu.Unlock()
	for _, s := range subs {
		s.finish(api.ListenerProviderClosed, api.ErrProviderClosed)
	}
}
func (s *subscription) unavailable() {
	s.mu.Lock()
	for i := range s.coverage {
		s.coverage[i].State = api.SourceUnavailable
		s.coverage[i].Cause = api.ErrLogSource
	}
	s.mu.Unlock()
	s.finish(api.ListenerSourceUnavailable, api.ErrLogSource)
}
func (s *subscription) Stop()                 { s.finish(api.ListenerStopped, nil) }
func (s *subscription) Done() <-chan struct{} { return s.done }
func (s *subscription) Coverage() []api.SourceStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]api.SourceStatus(nil), s.coverage...)
}
func (s *subscription) Wait(ctx context.Context) (api.ListenerResult, error) {
	select {
	case <-s.done:
		s.mu.Lock()
		defer s.mu.Unlock()
		r := s.result
		r.Coverage = append([]api.SourceStatus(nil), r.Coverage...)
		return r, r.Cause
	case <-ctx.Done():
		return api.ListenerResult{}, ctx.Err()
	}
}
func (s *subscription) finish(reason api.ListenerReason, cause error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return
	}
	s.ended = true
	for i := range s.coverage {
		c := &s.coverage[i]
		if c.State == api.SourcePending || c.State == api.SourceActive {
			c.State = api.SourceInterrupted
			c.Cause = cause
		}
	}
	s.result = api.ListenerResult{Reason: reason, Cause: cause, Coverage: append([]api.SourceStatus(nil), s.coverage...)}
	s.cancel()
	close(s.done)
}
func (s *subscription) mark(id string, state api.SourceState, cause error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return
	}
	for i := range s.coverage {
		c := &s.coverage[i]
		if c.LaunchID == id {
			c.State = state
			c.Closed = state == api.SourceClosed
			if c.Closed && s.pending[id+"/"+string(c.Source)] > 0 {
				c.State = api.SourceActive
			}
			c.Cause = cause
		}
	}
}
func invoke(fn func(context.Context, api.LaunchLog) error, ctx context.Context, r api.LaunchLog) (err error) {
	defer func() {
		if recover() != nil {
			err = api.ErrLogDelivery
		}
	}()
	return fn(ctx, r)
}
func (s *subscription) run() {
	// One callback worker retains its capacity until a blocked callback returns.
	type call struct {
		ctx context.Context
		r   api.LaunchLog
	}
	work := make(chan call)
	answers := make(chan error, 1)
	go func() {
		defer func() { m := s.manager; m.mu.Lock(); m.resident--; delete(m.subs, s); m.mu.Unlock() }()
		for c := range work {
			if s.ctx.Err() != nil {
				return
			}
			answers <- invoke(s.options.OnLog, c.ctx, c.r)
		}
	}()
	defer close(work)
	queue := make(chan api.LaunchLog, 64)
	pollDone := make(chan struct{})
	go func() { defer close(pollDone); s.poll(queue) }()
	defer func() { s.cancel(); <-pollDone }()
	var elapsed <-chan time.Time
	var timer *time.Timer
	if s.options.Duration > 0 {
		timer = time.NewTimer(max(time.Nanosecond, time.Until(s.registered.Add(s.options.Duration))))
		defer timer.Stop()
		elapsed = timer.C
	}
	canceled := func() {
		if errors.Is(s.ctx.Err(), context.DeadlineExceeded) {
			s.finish(api.ListenerContextDeadline, context.DeadlineExceeded)
		} else {
			s.finish(api.ListenerContextCanceled, context.Canceled)
		}
	}
	for {
		select {
		case <-s.ctx.Done():
			canceled()
			return
		case <-elapsed:
			s.finish(api.ListenerDurationElapsed, nil)
			return
		case r, ok := <-queue:
			if !ok {
				reason := api.ListenerSourcesClosed
				var cause error
				for _, c := range s.Coverage() {
					if c.State == api.SourceUnavailable {
						reason = api.ListenerSourceUnavailable
						cause = api.ErrLogSource
					}
					if c.State == api.SourceInterrupted {
						reason = api.ListenerSourceError
						cause = api.ErrLogSource
					}
				}
				s.finish(reason, cause)
				return
			}
			ctx, cancel := context.WithTimeout(s.ctx, s.manager.CallbackTimeout)
			s.mu.Lock()
			if s.ended {
				s.mu.Unlock()
				cancel()
				return
			}
			for i := range s.coverage {
				c := &s.coverage[i]
				if c.LaunchID == r.LaunchID && c.Source == r.Source {
					c.LastSequence = r.Sequence
					if c.State == api.SourcePending {
						c.State = api.SourceActive
					}
				}
			}
			s.mu.Unlock()
			select {
			case work <- call{ctx, r}:
			case <-s.ctx.Done():
				cancel()
				canceled()
				return
			}
			select {
			case err := <-answers:
				s.mu.Lock()
				key := r.LaunchID + "/" + string(r.Source)
				s.pending[key]--
				for i := range s.coverage {
					c := &s.coverage[i]
					if c.LaunchID == r.LaunchID && c.Source == r.Source && c.Closed && s.pending[key] == 0 {
						c.State = api.SourceClosed
					}
				}
				s.mu.Unlock()
				cancel()
				if errors.Is(err, api.ErrStopListening) {
					s.Stop()
					return
				}
				if err != nil {
					s.finish(api.ListenerError, api.ErrLogDelivery)
					return
				}
			case <-ctx.Done():
				cancel()
				if s.ctx.Err() != nil {
					canceled()
				} else {
					s.finish(api.ListenerSlow, api.ErrLogDelivery)
				}
				return
			case <-elapsed:
				cancel()
				s.finish(api.ListenerDurationElapsed, nil)
				return
			}
		}
	}
}
func (s *subscription) poll(queue chan<- api.LaunchLog) {
	defer close(queue)
	tokens := map[string]string{}
	closed := map[string]bool{}
	ticker := time.NewTicker(s.manager.PollInterval)
	defer ticker.Stop()
	for {
		all := true
		for _, id := range s.ids {
			if closed[id] {
				continue
			}
			all = false
			p, err := s.manager.Read(s.ctx, api.LaunchLogRequest{LaunchID: id, PageToken: tokens[id], PageSize: 64})
			if s.ctx.Err() != nil {
				return
			}
			if err != nil {
				s.mu.Lock()
				done := s.admissionDone
				s.mu.Unlock()
				if errors.Is(err, api.ErrLaunchLogNotFound) && !done {
					continue
				}
				s.mark(id, api.SourceUnavailable, api.ErrLogSource)
				closed[id] = true
				continue
			}
			for _, r := range p.Records {
				l := api.LaunchLog{LaunchID: id, SubscriptionID: fmt.Sprintf("%p", s), Sequence: r.Sequence, Timestamp: r.Timestamp, Source: r.Source, Data: r.Data}
				s.mu.Lock()
				s.pending[id+"/"+string(r.Source)]++
				s.mu.Unlock()
				select {
				case queue <- l:
				case <-s.ctx.Done():
					return
				default:
					s.finish(api.ListenerOverflow, api.ErrLogDelivery)
					return
				}
			}
			tokens[id] = p.NextPageToken
			if !p.HasMore && p.Finalized {
				state := api.SourceClosed
				var cause error
				if p.Incomplete {
					state = api.SourceInterrupted
					cause = api.ErrLogSource
				}
				s.mark(id, state, cause)
				closed[id] = true
			}
		}
		if all {
			return
		}
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
