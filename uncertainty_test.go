package docker

import (
	"context"
	"encoding/json"
	"fmt"
	provider "github.com/colony-2/pulse-provider-api"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestUncertaintyDoesNotReplayAfterMissingInventory(t *testing.T) {
	for _, test := range []struct {
		name string
		code int
		body string
		want provider.Status
	}{
		{"conflict", 409, `{"message":"synthetic-credential"}`, provider.Unknown},
		{"lost reply", 500, `{"message":"synthetic-credential"}`, provider.Unknown},
		{"missing ID", 200, `{}`, provider.Unknown},
		{"warnings", 200, `{"Id":"unseen","Warnings":["synthetic-credential"]}`, provider.Unknown},
		{"failed inspection", 200, `{"Id":"unseen"}`, provider.Unknown},
		{"definite rejection", 400, `{"message":"synthetic-credential"}`, provider.Rejected},
	} {
		t.Run(test.name, func(t *testing.T) {
			d := &daemon{containers: map[string]container{}}
			creates := 0
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/containers/create") {
					creates++
					w.WriteHeader(test.code)
					w.Write([]byte(test.body))
					return
				}
				d.serve(w, r)
			}))
			defer s.Close()
			n, err := open(t.Context(), Config{DependencyCache: CacheConfig{Disabled: true}}, &engine{base: s.URL, client: s.Client()}, false)
			if err != nil {
				t.Fatal(err)
			}
			p := wrapNative(n)
			defer p.Close()
			l := assignment("uncertain")
			for i := 0; i < 2; i++ {
				out, err := p.Submit(context.Background(), []provider.Launch{l})
				if err != nil || len(out) != 1 || out[0].Status != test.want || strings.Contains(out[0].Reason, "synthetic-credential") {
					t.Fatal(out, err)
				}
			}
			if test.want == provider.Unknown && creates != 1 {
				t.Fatal("uncertain launch replayed", creates)
			}
			if test.want == provider.Unknown && len(n.uncertain) != 1 {
				t.Fatal("missing inventory cleared reservation")
			}
		})
	}
}
func TestRegistryAuthenticationOnlyReachesImagePulls(t *testing.T) {
	requests := []string{}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Registry-Auth") != "" {
			if r.Header.Get("X-Registry-Auth") != "synthetic-registry-auth" {
				t.Error("changed auth")
			}
			requests = append(requests, r.URL.Path)
		}
		json.NewEncoder(w).Encode(map[string]any{})
	}))
	defer s.Close()
	e := &engine{base: s.URL, client: s.Client(), registryAuth: "synthetic-registry-auth"}
	if err := e.pull(t.Context(), "example.test/image:latest", "linux/arm64"); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/containers/create", "/containers/json", "/images/id/json", "/volumes/create"} {
		if err := e.call(t.Context(), "GET", path, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	if len(requests) != 1 || requests[0] != "/images/create" {
		t.Fatal("registry authentication escaped pull", requests)
	}
}

func TestDaemonErrorFormattingDoesNotExposeCredentials(t *testing.T) {
	secret := "synthetic-lease-echo"
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		json.NewEncoder(w).Encode(map[string]string{"message": secret})
	}))
	defer s.Close()
	n := &engineProvider{e: &engine{base: s.URL, client: s.Client()}, platform: "linux/arm64"}
	p := wrapNative(n)
	defer p.Close()
	_, err := p.List(t.Context(), provider.ListRequest{})
	if err == nil {
		t.Fatal("expected daemon failure")
	}
	for _, format := range []string{"%v", "%+v", "%#v", "%s"} {
		if strings.Contains(fmt.Sprintf(format, err), secret) {
			t.Fatal("credential in formatted daemon error", format)
		}
	}
}

func TestRegistryEchoIsRedactedFromProbeDiagnostics(t *testing.T) {
	const auth = "synthetic-encoded-registry-auth"
	for _, status := range []int{200, 500} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
				if status == 200 {
					json.NewEncoder(w).Encode(map[string]string{"error": auth})
				} else {
					json.NewEncoder(w).Encode(map[string]string{"message": auth})
				}
			}))
			defer s.Close()
			e := &engine{base: s.URL, client: s.Client(), registryAuth: auth}
			err := e.pull(t.Context(), "example.test/image:latest", "linux/arm64")
			if err == nil {
				t.Fatal("expected pull failure")
			}
			diagnostic := probeDiagnostic("resolve image", err)
			for _, format := range []string{"%v", "%+v", "%#v", "%s"} {
				if strings.Contains(fmt.Sprintf(format, diagnostic), auth) {
					t.Fatal("registry auth escaped probe diagnostic", format)
				}
			}
		})
	}
}
