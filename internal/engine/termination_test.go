package engine

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDockerTerminationEvidence(t *testing.T) {
	for _, response := range []struct {
		body  string
		code  int
		valid bool
	}{
		{`{"StatusCode":0}`, 0, true}, {`{"StatusCode":7,"Error":null}`, 7, true},
		{`{}`, 0, false}, {`{"StatusCode":0,"Error":{"Message":"private daemon body"}}`, 0, false},
		{`{"StatusCode":0} {}`, 0, false}, {`{"StatusCode":null}`, 0, false},
	} {
		t.Run(response.body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Method != "POST" || req.URL.Path != "/containers/id/wait" || req.URL.Query().Get("condition") != "not-running" {
					t.Errorf("unexpected wait: %s %s", req.Method, req.URL)
				}
				fmt.Fprint(w, response.body)
			}))
			defer server.Close()
			engine := Engine{Base: server.URL, Client: server.Client()}
			code, err := engine.WaitWorker(t.Context(), "id")
			if (err == nil) != response.valid || code != response.code || err != nil && strings.Contains(err.Error(), "private") {
				t.Fatal(code, err)
			}
		})
	}
	zero := 0
	for _, state := range []State{{Status: "created", ExitCode: &zero}, {Status: "exited", ExitCode: &zero}, {Status: "exited", StartedAt: "invalid", ExitCode: &zero}, {Status: "exited", StartedAt: "2026-11-23T00:00:00Z"}} {
		if state.Terminal() {
			t.Fatalf("unsupported terminal result: %+v", state)
		}
	}
}
