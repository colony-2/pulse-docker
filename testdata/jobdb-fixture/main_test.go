package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestFixtureGateLifecycle(t *testing.T) {
	for _, manual := range []bool{false, true} {
		name := "automatic release after heartbeat"
		if manual {
			name = "held across heartbeats until explicit release"
		}
		t.Run(name, func(t *testing.T) {
			api := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
			server := httptest.NewServer(newFixtureHandler(api, http.NotFoundHandler(), "fixture-hash"))
			defer server.Close()
			request := func(method, path string) {
				t.Helper()
				req, err := http.NewRequestWithContext(t.Context(), method, server.URL+path, nil)
				if err != nil {
					t.Fatal(err)
				}
				res, err := server.Client().Do(req)
				if err != nil {
					t.Fatal(err)
				}
				defer res.Body.Close()
				_, err = io.Copy(io.Discard, res.Body)
				if err != nil || res.StatusCode != http.StatusOK {
					t.Fatalf("%s %s: status=%s error=%v", method, path, res.Status, err)
				}
			}
			if manual {
				request("POST", "/control/hold-gate")
			}
			request("POST", "/control/observed")
			for range 4 {
				request("POST", "/fixture/keepalive")
			}
			if manual {
				// A real client must stay blocked even after the automatic-release
				// threshold. Cancel it before releasing the recovery fixture.
				ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
				defer cancel()
				req, err := http.NewRequestWithContext(ctx, "GET", server.URL+"/gate", nil)
				if err != nil {
					t.Fatal(err)
				}
				res, err := server.Client().Do(req)
				if res != nil {
					res.Body.Close()
				}
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("held gate released before recovery completed: %v", err)
				}
				request("POST", "/control/release-gate")
				request("POST", "/control/release-gate")
			}
			request("GET", "/gate")
		})
	}
}
