package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInspectionReadOnlyAlongsideOwner(t *testing.T) {
	d := &daemon{containers: map[string]container{}}
	n, server := testProvider(t, d)
	defer server.Close()
	if out, err := n.Submit(t.Context(), launches(requests("one"))); err != nil || len(out) != 1 {
		t.Fatal(out, err)
	}
	dir, err := os.MkdirTemp("", "inspect-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "docker.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	reads := []string{}
	httpServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Error("inspection mutated daemon", r.Method, r.URL)
			w.WriteHeader(500)
			return
		}
		reads = append(reads, r.URL.Path)
		if strings.HasSuffix(r.URL.Path, "/images/json") {
			json.NewEncoder(w).Encode([]map[string]any{{"Id": "sha256:config", "RepoTags": []string{"runner:1"}, "Created": 123}, {"Id": "sha256:unrelated", "RepoTags": []string{"unrelated:latest"}, "Created": 123}})
			return
		}
		d.serve(w, r)
	}))
	httpServer.Listener.Close()
	httpServer.Listener = listener
	httpServer.Start()
	defer httpServer.Close()
	cfg := Config{Socket: "unix://" + socket, LockDir: t.TempDir()}
	owner, err := open(t.Context(), cfg, &engine{Base: server.URL, Client: server.Client()}, true)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	view, images, err := Inspect(t.Context(), cfg)
	if err != nil || len(view.Containers) != 1 || len(images) != 1 || images[0].ID != "sha256:config" || view.Observed.MaxContainers != 1 || view.Configured.CPUMillis != 0 || view.Containers[0].C2JVersion != "unknown" {
		t.Fatal(view, images, err)
	}
	for _, path := range reads {
		if strings.Contains(path, "volumes") || strings.Contains(path, "create") {
			t.Fatal("inspection ran maintenance", path)
		}
	}
	if other, err := open(t.Context(), cfg, &engine{Base: server.URL, Client: server.Client()}, true); err == nil {
		other.Close()
		t.Fatal("inspection released owner")
	}
}
func TestRefreshExplicitPullAndPartialFailure(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(map[bool]string{false: "all verified", true: "partial failure"}[failure], func(t *testing.T) {
			d := &daemon{apiVersion: "1.49", multiPlatform: true, containers: map[string]container{}}
			pulls := []string{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/images/create") {
					platform := r.URL.Query().Get("platform")
					pulls = append(pulls, platform)
					if failure && platform == "linux/amd64" {
						w.WriteHeader(500)
						return
					}
					json.NewEncoder(w).Encode(map[string]string{"status": "Digest: sha256:" + strings.Repeat("a", 64)})
					return
				}
				if strings.Contains(r.URL.Path, "/images/") && strings.HasSuffix(r.URL.Path, "/json") {
					record := httptest.NewRecorder()
					d.serve(record, r)
					var image map[string]any
					if json.Unmarshal(record.Body.Bytes(), &image) != nil {
						t.Fatal("bad fixture image")
					}
					image["RepoDigests"] = []string{"ghcr.io/colony-2/base2@sha256:" + strings.Repeat("a", 64)}
					b, _ := json.Marshal(image)
					io.Copy(w, bytes.NewReader(b))
					return
				}
				d.serve(w, r)
			}))
			defer server.Close()
			n, err := open(t.Context(), Config{}, &engine{Base: server.URL, Client: server.Client()}, false)
			if err != nil {
				t.Fatal(err)
			}
			p := wrapNative(n)
			p.platforms = []string{"linux/arm64", "linux/amd64", "linux/arm64"}
			defer p.Close()
			var updates []ImageUpdate
			err = p.RefreshImages(context.Background(), func(u ImageUpdate) { updates = append(updates, u) })
			if (err != nil) != failure || len(pulls) != 2 || len(updates) != map[bool]int{true: 1, false: 2}[failure] {
				t.Fatal(pulls, updates, err)
			}
			for _, u := range updates {
				if u.PreviousID != "sha256:config" || u.ID != "sha256:config" {
					t.Fatal(u)
				}
			}
			if len(d.creates) != 0 || len(d.containers) != 0 {
				t.Fatal("refresh changed worker resources")
			}
			if errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		})
	}
}
func TestRefreshRejectsUnverifiedImageWithoutAnotherPull(t *testing.T) {
	for _, failure := range []string{"platform", "digest", "missing digest"} {
		t.Run(failure, func(t *testing.T) {
			d := &daemon{containers: map[string]container{}}
			pulls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/images/create") {
					pulls++
					if failure == "missing digest" {
						w.Write([]byte(`{"status":"pulled"}`))
					} else {
						json.NewEncoder(w).Encode(map[string]string{"status": "Digest: sha256:" + strings.Repeat("a", 64)})
					}
					return
				}
				if strings.Contains(r.URL.Path, "/images/") && strings.HasSuffix(r.URL.Path, "/json") {
					image := map[string]any{"Id": "sha256:config", "Os": "linux", "Architecture": "arm64", "RepoDigests": []string{"ghcr.io/colony-2/base2@sha256:" + strings.Repeat("a", 64)}}
					if failure == "platform" {
						image["Architecture"] = "amd64"
					}
					if failure == "digest" {
						image["RepoDigests"] = []string{"ghcr.io/colony-2/base2@sha256:" + strings.Repeat("b", 64)}
					}
					json.NewEncoder(w).Encode(image)
					return
				}
				d.serve(w, r)
			}))
			defer server.Close()
			n, err := open(t.Context(), Config{}, &engine{Base: server.URL, Client: server.Client()}, false)
			if err != nil {
				t.Fatal(err)
			}
			p := wrapNative(n)
			defer p.Close()
			updates := 0
			if err = p.RefreshImages(t.Context(), func(ImageUpdate) { updates++ }); err == nil || pulls != 1 || updates != 0 {
				t.Fatal("unverified refresh or repeated pull", err, pulls, updates)
			}
		})
	}
}
