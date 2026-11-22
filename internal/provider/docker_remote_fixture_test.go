//go:build integration

package provider

import (
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// Keep the last successful inventory for diagnostics; a failed Docker query
// cannot establish removal. The caller bounds observation through its context.
func waitRemoteFixtureRemoval(ctx context.Context, list func(context.Context) (string, error)) (string, error) {
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	var remaining string
	for {
		if err := ctx.Err(); err != nil {
			return remaining, err
		}
		ids, err := list(ctx)
		if err != nil {
			return remaining, err
		}
		remaining = strings.TrimSpace(ids)
		if remaining == "" {
			return "", nil
		}
		select {
		case <-ctx.Done():
			return remaining, ctx.Err()
		case <-tick.C:
		}
	}
}

func TestDockerRemoteFixtureRemovalObservation(t *testing.T) {
	t.Run("waits for a successful empty inventory", func(t *testing.T) {
		calls := 0
		ids, err := waitRemoteFixtureRemoval(t.Context(), func(context.Context) (string, error) {
			calls++
			if calls == 1 {
				return "worker\n", nil
			}
			return "\n", nil
		})
		if err != nil || ids != "" || calls != 2 {
			t.Fatalf("removal not observed: ids=%q calls=%d error=%v", ids, calls, err)
		}
	})
	t.Run("retains surviving worker when observation ends", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		ids, err := waitRemoteFixtureRemoval(ctx, func(context.Context) (string, error) {
			cancel()
			return "worker\n", nil
		})
		if ids != "worker" || !errors.Is(err, context.Canceled) {
			t.Fatalf("surviving worker passed: ids=%q error=%v", ids, err)
		}
	})
	t.Run("query failure does not establish removal", func(t *testing.T) {
		failure := errors.New("daemon unavailable")
		calls := 0
		ids, err := waitRemoteFixtureRemoval(t.Context(), func(context.Context) (string, error) {
			calls++
			if calls == 1 {
				return "worker", nil
			}
			return "", failure
		})
		if ids != "worker" || !errors.Is(err, failure) {
			t.Fatalf("query failure passed: ids=%q error=%v", ids, err)
		}
	})
	t.Run("persistent worker fails at deadline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			ids, err := waitRemoteFixtureRemoval(ctx, func(context.Context) (string, error) {
				return "worker\n", nil
			})
			if ids != "worker" || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("persistent worker passed: ids=%q error=%v", ids, err)
			}
		})
	})
}
