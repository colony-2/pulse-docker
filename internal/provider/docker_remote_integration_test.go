//go:build integration

package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/colony-2/pulse-docker/internal/launchlogs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/colony-2/pulse-docker/internal/bases"
	compute "github.com/colony-2/pulse-docker/internal/native"
	api "github.com/colony-2/pulse-provider-api"
	"github.com/colony-2/pulse-provider-api/plugin"
)

func remoteBinary(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pulse-docker")
	cmd := exec.CommandContext(t.Context(), "go", "build", "-o", path, "../../cmd/pulse-docker")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build remote program: %v\n%s", err, b)
	}
	return path
}
func TestDockerRemoteInboundLeaseIntegration(t *testing.T) {
	testDockerLeaseScenarios(t, remoteOpener(t, "inbound", remoteBinary(t)), []string{"success and heartbeat"})
}
func TestDockerRemoteOutboundLeaseIntegration(t *testing.T) {
	testDockerLeaseScenarios(t, remoteOpener(t, "outbound", remoteBinary(t)), []string{"success and heartbeat"})
}

type restartClient struct {
	api.Provider
	restart func() api.Provider
}

func (p *restartClient) Submit(ctx context.Context, ls []api.Launch, o api.SubmitOptions) ([]api.Submission, api.LogSubscription, error) {
	out, sub, err := p.Provider.Submit(ctx, ls, o)
	if err == nil && len(out) == 1 && out[0].Status == api.Accepted {
		p.Provider = p.restart()
		page, e := p.Provider.List(ctx, api.ListRequest{LaunchID: ls[0].LaunchID})
		if e != nil || len(page.Items) != 1 {
			return out, sub, fmt.Errorf("accepted worker disappeared during server restart: %w", e)
		}
	}
	return out, sub, err
}
func remoteOpener(t *testing.T, mode, binary string) func(*testing.T, context.Context, Config) (api.Provider, func()) {
	return func(t *testing.T, ctx context.Context, cfg Config) (api.Provider, func()) {
		home, err := filepath.Abs(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		args := []string{"server", "--mode", mode, "--owner", "fixture", "--socket", cfg.Socket, "--lock-dir", cfg.LockDir, "--cache-disabled", "--log-directory", filepath.Join(home, "logs"), "--shutdown-timeout", "10s", "--startup-timeout", "10m"}
		environment := map[string]string{}
		for _, entry := range os.Environ() {
			k, v, ok := strings.Cut(entry, "=")
			if ok {
				environment[k] = v
			}
		}
		environment["HOME"] = home
		if mode == "inbound" {
			var process *plugin.Process
			start := func() api.Provider {
				var err error
				process, err = plugin.Start(ctx, plugin.StartOptions{Executable: binary, Args: args, Env: environment, Init: plugin.Initialization{Name: "fixture", BaseDir: home}, StartupTimeout: 10 * time.Minute, CallTimeout: 10 * time.Minute, ShutdownTimeout: 15 * time.Second})
				if err != nil {
					t.Fatal(err)
				}
				return process
			}
			initial := start()
			return &restartClient{Provider: initial, restart: func() api.Provider {
					if err := process.Close(); err != nil {
						t.Fatal(err)
					}
					return start()
				}}, func() {
					if err := process.Close(); err != nil {
						t.Error(err)
					}
				}
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		collector, err := plugin.StartCollector(ctx, listener, plugin.CollectorOptions{AuthMode: plugin.ReverseAuthMutualKey, Transport: plugin.ReverseTransportHTTP, MutualKeys: map[string][]byte{"fixture": []byte("isolated-fixture-key")}, StartupTimeout: 10 * time.Minute, CallTimeout: 10 * time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		key := filepath.Join(home, "key")
		os.WriteFile(key, []byte("isolated-fixture-key"), 0600)
		args = append(args, "--collector", listener.Addr().String(), "--reverse-auth", "mutual-key", "--reverse-transport", "http", "--shared-key-file", key)
		var cmd *exec.Cmd
		var done chan error
		var logFile *os.File
		stop := func() {
			if cmd == nil {
				return
			}
			_ = cmd.Process.Signal(syscall.SIGTERM)
			select {
			case err := <-done:
				if err != nil {
					t.Error("server shutdown", err)
				}
			case <-time.After(20 * time.Second):
				cmd.Process.Kill()
				t.Error("server shutdown timed out")
			}
			logFile.Close()
			cmd = nil
		}
		start := func(after uint64) api.Provider {
			cmd = exec.CommandContext(ctx, binary, args...)
			cmd.Env = os.Environ()
			logFile, err = os.Create(filepath.Join(home, fmt.Sprintf("server-%d.log", after)))
			if err != nil {
				t.Fatal(err)
			}
			cmd.Stdout = logFile
			cmd.Stderr = logFile
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			done = make(chan error, 1)
			go func(process *exec.Cmd) { done <- process.Wait() }(cmd)
			for {
				snap, err := collector.Snapshot()
				if err != nil {
					t.Fatal(err)
				}
				for _, p := range snap.Providers {
					if p.Generation > after {
						return p.Provider
					}
				}
				if _, err = collector.WaitForChange(ctx, snap.Revision); err != nil {
					t.Fatal(err)
				}
			}
		}
		initial := start(0)
		return &restartClient{Provider: initial, restart: func() api.Provider {
			snapshot, _ := collector.Snapshot()
			after := snapshot.Providers[0].Generation
			stop()
			return start(after)
		}}, func() { stop(); collector.Close() }
	}
}
func liveDocker(t *testing.T) (context.Context, Config, func(...string) string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Minute)
	t.Cleanup(cancel)
	socket, err := ResolveSocket("")
	if err != nil {
		t.Fatal(err)
	}
	docker := func(args ...string) string {
		t.Helper()
		cmd := exec.CommandContext(ctx, "docker", append([]string{"--host", socket}, args...)...)
		b, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("docker %v: %v\n%s", args, err, b)
		}
		return strings.TrimSpace(string(b))
	}
	docker("version")
	return ctx, Config{Socket: socket, LockDir: t.TempDir(), DependencyCache: CacheConfig{Disabled: true}}, docker
}
func TestDockerFastCaptureIntegration(t *testing.T) {
	ctx, cfg, docker := liveDocker(t)
	platform := strings.ReplaceAll(strings.ReplaceAll(docker("info", "--format", "{{.OSType}}/{{.Architecture}}"), "aarch64", "arm64"), "x86_64", "amd64")
	directory := t.TempDir()
	p, err := NewWithOptions(ctx, cfg, Options{Logging: &LoggingConfig{Directory: directory, Owner: "fixture", Compress: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	id := fmt.Sprintf("fast-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		ids, err := exec.CommandContext(cleanup, "docker", "--host", cfg.Socket, "ps", "-aq", "--filter", "label=pulse_launch_id="+id).CombinedOutput()
		if err != nil {
			t.Errorf("find fast-capture fixture for cleanup: %v %s", err, ids)
			return
		}
		if remaining := strings.Fields(string(ids)); len(remaining) > 0 {
			if b, err := exec.CommandContext(cleanup, "docker", append([]string{"--host", cfg.Socket, "rm", "-f"}, remaining...)...).CombinedOutput(); err != nil {
				t.Errorf("clean up fast-capture fixture: %v %s", err, b)
			}
		}
	})
	p.capture.store.Begin(launchHistory(id))
	ls := []compute.Launch{{Request: compute.Request{LaunchID: id, Allocation: compute.Allocation{Image: "alpine:3.21", Platform: platform}, Metadata: map[string]string{"pulse_job_id": id}}, Process: compute.Process{Command: []string{"/bin/sh"}, Args: []string{"-c", "printf '\\000\\377fast'; printf 'stderr' >&2"}}}}
	out, err := p.native.Submit(ctx, ls)
	p.capture.admitted(id)
	if err != nil || len(out) != 1 || out[0].Status != compute.Accepted {
		t.Fatal(out, err)
	}
	page := waitFinalLog(t, ctx, p, id)
	var raw []byte
	for _, r := range page.Records {
		raw = append(raw, r.Data...)
	}
	if string(raw) != string([]byte{0, 255})+"faststderr" || page.Incomplete {
		t.Fatal("fast output changed", raw, page)
	}
	// Output EOF and Docker's asynchronous AutoRemove do not complete atomically.
	// Require actual disappearance before reopening the archive, without removing
	// the worker ourselves until test cleanup.
	removal, cancel := context.WithTimeout(ctx, 15*time.Second)
	ids, err := waitRemoteFixtureRemoval(removal, func(check context.Context) (string, error) {
		b, err := exec.CommandContext(check, "docker", "--host", cfg.Socket, "ps", "-aq", "--filter", "label=pulse_launch_id="+id).CombinedOutput()
		if err != nil {
			return "", fmt.Errorf("query worker removal: %w: %s", err, b)
		}
		return string(b), nil
	})
	cancel()
	if err != nil {
		diagnostic, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		state, inspectErr := exec.CommandContext(diagnostic, "docker", append([]string{"--host", cfg.Socket, "inspect", "--format", "{{json .State}} autoremove={{.HostConfig.AutoRemove}}"}, strings.Fields(ids)...)...).CombinedOutput()
		t.Fatalf("worker was not automatically removed: %v; last inventory=%q; inspect error=%v; state=%s", err, ids, inspectErr, state)
	}
	p.Close()
	p, err = NewWithOptions(ctx, cfg, Options{Logging: &LoggingConfig{Directory: directory, Owner: "fixture", Compress: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if again, err := p.GetLaunchLog(ctx, api.LaunchLogRequest{LaunchID: id}); err != nil || len(again.Records) != len(page.Records) {
		t.Fatal(again, err)
	}
}
func TestDockerCaptureRestartIntegration(t *testing.T) {
	ctx, cfg, docker := liveDocker(t)
	platform := strings.ReplaceAll(strings.ReplaceAll(docker("info", "--format", "{{.OSType}}/{{.Architecture}}"), "aarch64", "arm64"), "x86_64", "amd64")
	directory := t.TempDir()
	options := Options{Logging: &LoggingConfig{Directory: directory, Owner: "fixture"}}
	p, err := NewWithOptions(ctx, cfg, options)
	if err != nil {
		t.Fatal(err)
	}
	id := fmt.Sprintf("restart-%d", time.Now().UnixNano())
	p.capture.store.Begin(launchHistory(id))
	l := compute.Launch{Request: compute.Request{LaunchID: id, Allocation: compute.Allocation{Image: "alpine:3.21", Platform: platform}, Metadata: map[string]string{"pulse_job_id": id}}, Process: compute.Process{Command: []string{"/bin/sh"}, Args: []string{"-c", "echo prefix; sleep 5; echo suffix; sleep 300"}}}
	out, err := p.native.Submit(ctx, []compute.Launch{l})
	p.capture.admitted(id)
	if err != nil || out[0].Status != compute.Accepted {
		t.Fatal(out, err)
	}
	history, _ := p.capture.store.Histories()
	container := history[0].ContainerID
	t.Cleanup(func() { exec.Command("docker", "--host", cfg.Socket, "rm", "-f", container).Run() })
	waitLogText(t, ctx, p, id, "prefix")
	p.Close()
	p, err = NewWithOptions(ctx, cfg, options)
	if err != nil {
		t.Fatal(err)
	}
	waitLogText(t, ctx, p, id, "suffix")
	p.Close()
	docker("rm", "-f", container)
	p, err = NewWithOptions(ctx, cfg, options)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	page := waitFinalLog(t, ctx, p, id)
	if !page.Incomplete {
		t.Fatal("downtime removal not disclosed")
	}
	var raw string
	for _, r := range page.Records {
		raw += string(r.Data)
	}
	if strings.Count(raw, "prefix") != 1 || strings.Count(raw, "suffix") != 1 {
		t.Fatal("recovery duplicated output", raw)
	}
}
func TestDockerManagementIntegration(t *testing.T) {
	ctx, cfg, docker := liveDocker(t)
	binary := remoteBinary(t)
	cfg.DependencyCache = CacheConfig{Namespace: fmt.Sprintf("refresh-%d", time.Now().UnixNano()), Generation: "fixture", MaxAge: time.Hour}
	p, err := New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	original := docker("image", "inspect", "--format", "{{.Id}}", bases.Image)
	id := fmt.Sprintf("refresh-worker-%d", time.Now().UnixNano())
	platform := p.platforms[0]
	l := compute.Launch{Request: compute.Request{LaunchID: id, Allocation: compute.Allocation{Image: bases.Image, Platform: platform, MemoryBytes: 256 << 20}, Metadata: map[string]string{"pulse_jobdb_instance_id": "fixture", "pulse_tenant_id": "fixture", "pulse_job_id": id}}, Process: compute.Process{Command: []string{"/bin/sh"}, Args: []string{"-c", "echo refresh-marker > /var/cache/pulse/refresh-marker; sleep 300"}}}
	t.Cleanup(func() {
		ids, _ := exec.Command("docker", "--host", cfg.Socket, "ps", "-aq", "--filter", "label=pulse_launch_id="+id).Output()
		for _, container := range strings.Fields(string(ids)) {
			exec.Command("docker", "--host", cfg.Socket, "rm", "-f", container).Run()
		}
		volumes, _ := exec.Command("docker", "--host", cfg.Socket, "volume", "ls", "-q", "--filter", "label=pulse_cache_owner="+p.native.cacheOwner()).Output()
		for _, volume := range strings.Fields(string(volumes)) {
			exec.Command("docker", "--host", cfg.Socket, "volume", "rm", volume).Run()
		}
	})
	out, err := p.native.Submit(ctx, []compute.Launch{l})
	if err != nil || len(out) != 1 || out[0].Status != compute.Accepted {
		t.Fatal(out, err)
	}
	container := docker("ps", "-q", "--filter", "label=pulse_launch_id="+id)
	if container == "" {
		t.Fatal("holding worker missing")
	}
	marker := docker("exec", container, "sh", "-ec", "i=0; until test -f /var/cache/pulse/refresh-marker; do i=$((i+1)); test $i -lt 100; sleep 0.1; done; cat /var/cache/pulse/refresh-marker")
	if marker != "refresh-marker" {
		t.Fatal(marker)
	}
	for _, command := range []string{"status", "images"} {
		cmd := exec.CommandContext(ctx, binary, command, "--socket", cfg.Socket, "--lock-dir", cfg.LockDir, "--json")
		b, err := cmd.CombinedOutput()
		if err != nil || !json.Valid(b) {
			t.Fatalf("%s beside provider: %v %s", command, err, b)
		}
	}
	sentinel := fmt.Sprintf("refresh-unrelated-%d", time.Now().UnixNano())
	docker("volume", "create", sentinel)
	t.Cleanup(func() { exec.Command("docker", "--host", cfg.Socket, "volume", "rm", sentinel).Run() })
	cacheBefore := p.native.cfg.DependencyCache
	var updates []ImageUpdate
	if err = p.RefreshImages(ctx, func(u ImageUpdate) { updates = append(updates, u) }); err != nil {
		t.Fatal(err)
	}
	if len(updates) != len(bases.Catalog(p.platforms...)) || updates[0].PreviousID != original {
		t.Fatal(updates)
	}
	if p.native.cfg.DependencyCache != cacheBefore {
		t.Fatal("refresh changed cache generation")
	}
	if image := docker("inspect", "--format", "{{.Config.Image}}", container); image != original {
		t.Fatal("refresh changed accepted worker image", image, original)
	}
	if marker := docker("exec", container, "cat", "/var/cache/pulse/refresh-marker"); marker != "refresh-marker" {
		t.Fatal("refresh changed cache marker", marker)
	}
	docker("volume", "inspect", sentinel)
}

func launchHistory(id string) launchlogs.History {
	return launchlogs.History{LaunchID: id, InstanceID: "fixture", TenantID: "test", JobID: id, SubmittedAt: time.Now().UTC()}
}
func waitFinalLog(t *testing.T, ctx context.Context, p *Provider, id string) api.LaunchLogResponse {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		page, err := p.GetLaunchLog(ctx, api.LaunchLogRequest{LaunchID: id})
		if err != nil {
			t.Fatal(err)
		}
		if page.Finalized {
			return page
		}
		if time.Now().After(deadline) {
			t.Fatal("capture did not finalize")
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}
func waitLogText(t *testing.T, ctx context.Context, p *Provider, id, text string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		page, err := p.GetLaunchLog(ctx, api.LaunchLogRequest{LaunchID: id})
		if err != nil {
			t.Fatal(err)
		}
		var raw string
		for _, r := range page.Records {
			raw += string(r.Data)
		}
		if strings.Contains(raw, text) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("capture did not contain %s: %s", text, raw)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}
