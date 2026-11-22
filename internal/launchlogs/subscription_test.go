package launchlogs

import (
	"context"
	"errors"
	api "github.com/colony-2/pulse-provider-api"
	"sync"
	"testing"
	"time"
)

func waitSub(t *testing.T, s api.LogSubscription) api.ListenerResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	r, err := s.Wait(ctx)
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("subscription stuck")
	}
	return r
}
func TestSubscriptionNilDisabledAndValidation(t *testing.T) {
	m := New(nil)
	ls := []api.Launch{{LaunchID: "one"}}
	if s := m.Subscribe(ls, api.SubmitOptions{}); s != nil || m.resident != 0 {
		t.Fatal("nil listener allocated")
	}
	s := m.Subscribe(ls, api.SubmitOptions{Listener: &api.ListenOptions{Context: t.Context(), OnLog: func(context.Context, api.LaunchLog) error { t.Fatal("unavailable source callback"); return nil }}})
	if r := waitSub(t, s); r.Reason != api.ListenerSourceUnavailable {
		t.Fatal(r)
	}
	for _, l := range []api.ListenOptions{{}, {Context: t.Context()}, {Context: t.Context(), OnLog: func(context.Context, api.LaunchLog) error { return nil }, Duration: -1}} {
		if Validate(api.SubmitOptions{Listener: &l}) == nil {
			t.Fatal("invalid listener accepted")
		}
	}
}
func TestSubscriptionStableRecordsAndNaturalDrain(t *testing.T) {
	s, _ := archiveFixture(t)
	s.Append("one", api.LogSourceStdout, []byte("early"))
	s.Append("one", api.LogSourceStderr, []byte("late"))
	s.Finish("one", true, false)
	m := New(s.GetLaunchLog)
	defer m.Close()
	var mu sync.Mutex
	var seen []api.LaunchLog
	sub := m.Subscribe([]api.Launch{{LaunchID: "one"}}, api.SubmitOptions{Listener: &api.ListenOptions{Context: t.Context(), OnLog: func(_ context.Context, r api.LaunchLog) error {
		mu.Lock()
		seen = append(seen, r)
		mu.Unlock()
		return nil
	}}})
	r := waitSub(t, sub)
	mu.Lock()
	defer mu.Unlock()
	if r.Reason != api.ListenerSourcesClosed || len(seen) != 2 || seen[0].Sequence != 1 || seen[1].Sequence != 2 || seen[1].Source != api.LogSourceStderr {
		t.Fatal(r, seen)
	}
	r.Coverage[0].State = "mutated"
	if sub.Coverage()[0].State == "mutated" {
		t.Fatal("shared coverage")
	}
}
func TestSubscriptionIndependentStopWaitAndDuration(t *testing.T) {
	store, _ := archiveFixture(t)
	m := New(store.GetLaunchLog)
	defer m.Close()
	sub := m.Subscribe([]api.Launch{{LaunchID: "one"}}, api.SubmitOptions{Listener: &api.ListenOptions{Context: t.Context(), OnLog: func(context.Context, api.LaunchLog) error { return nil }}})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := sub.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	select {
	case <-sub.Done():
		t.Fatal("Wait cancellation stopped subscription")
	default:
	}
	sub.Stop()
	if r := waitSub(t, sub); r.Reason != api.ListenerStopped {
		t.Fatal(r)
	}
	sub = m.Subscribe([]api.Launch{{LaunchID: "one"}}, api.SubmitOptions{Listener: &api.ListenOptions{Context: t.Context(), Duration: 5 * time.Millisecond, OnLog: func(context.Context, api.LaunchLog) error { return nil }}})
	if r := waitSub(t, sub); r.Reason != api.ListenerDurationElapsed {
		t.Fatal(r)
	}
}
func TestSubscriptionCallbackFailureAndBoundedResidence(t *testing.T) {
	for _, kind := range []string{"stop", "error", "panic", "blocked"} {
		t.Run(kind, func(t *testing.T) {
			store, _ := archiveFixture(t)
			store.Append("one", api.LogSourceStdout, []byte("x"))
			store.Finish("one", true, false)
			m := New(store.GetLaunchLog)
			m.CallbackTimeout = 10 * time.Millisecond
			defer m.Close()
			release := make(chan struct{})
			defer close(release)
			sub := m.Subscribe([]api.Launch{{LaunchID: "one"}}, api.SubmitOptions{Listener: &api.ListenOptions{Context: t.Context(), OnLog: func(context.Context, api.LaunchLog) error {
				switch kind {
				case "stop":
					return api.ErrStopListening
				case "error":
					return errors.New("private callback data")
				case "panic":
					panic("private")
				case "blocked":
					<-release
				}
				return nil
			}}})
			r := waitSub(t, sub)
			if kind == "blocked" {
				if r.Reason != api.ListenerSlow {
					t.Fatal(r)
				}
				m.mu.Lock()
				resident := m.resident
				m.mu.Unlock()
				if resident != 1 {
					t.Fatal("uncooperative callback slot freed")
				}
			} else if kind == "stop" {
				if r.Reason != api.ListenerStopped {
					t.Fatal(r)
				}
			} else if r.Reason != api.ListenerError {
				t.Fatal(r)
			}
		})
	}
}
func TestSubscriptionStopRetainsNativeClosureWithInterruptedDelivery(t *testing.T) {
	store, _ := archiveFixture(t)
	store.Append("one", api.LogSourceStdout, []byte("first"))
	store.Append("one", api.LogSourceStdout, []byte("second"))
	store.Finish("one", true, false)
	m := New(store.GetLaunchLog)
	defer m.Close()
	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	sub := m.Subscribe([]api.Launch{{LaunchID: "one"}}, api.SubmitOptions{Listener: &api.ListenOptions{Context: t.Context(), OnLog: func(context.Context, api.LaunchLog) error { close(entered); <-release; return nil }}})
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("no callback")
	}
	deadline := time.Now().Add(time.Second)
	for {
		nativeClosed := false
		for _, c := range sub.Coverage() {
			nativeClosed = nativeClosed || c.Source == api.LogSourceStdout && c.Closed
		}
		if nativeClosed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("native closure not observed")
		}
		time.Sleep(time.Millisecond)
	}
	sub.Stop()
	r := waitSub(t, sub)
	if r.Reason != api.ListenerStopped {
		t.Fatal(r)
	}
	for _, c := range r.Coverage {
		if c.Source == api.LogSourceStdout && (!c.Closed || c.State != api.SourceInterrupted) {
			t.Fatal("closed native source concealed undelivered records", c)
		}
	}
}
