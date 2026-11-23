//go:build integration

package provider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/colony-2/pulse-docker/internal/bases"
	"github.com/colony-2/pulse-docker/internal/launchlogs"
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

func (p *restartClient) GetLaunchLog(ctx context.Context, request api.LaunchLogRequest) (api.LaunchLogResponse, error) {
	logs, ok := p.Provider.(api.ProviderWithLaunchLogs)
	if !ok {
		return api.LaunchLogResponse{}, api.ErrLaunchLogUnsupported
	}
	return logs.GetLaunchLog(ctx, request)
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
	for _, scenario := range []struct {
		name, script   string
		exit           int
		oom            bool
		stdout, stderr []byte
	}{
		{"exit_zero", "printf '\\000\\377fast'; printf 'stderr' >&2", 0, false, append([]byte{0, 255}, []byte("fast")...), []byte("stderr")},
		{"exit_nonzero", "printf 'failed'; exit 7", 7, false, []byte("failed"), nil},
		{"oom", `awk 'BEGIN { for (i=0;;i++) a[i]=sprintf("%01024d",i) }' &
child=$!; result=0; wait "$child" 2>/dev/null || result=$?; sleep 1; exit "$result"`, 137, true, nil, nil},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			testDockerFastCapture(t, scenario.script, scenario.exit, scenario.oom, scenario.stdout, scenario.stderr)
		})
	}
}

func testDockerFastCapture(t *testing.T, script string, code int, oom bool, stdout, stderr []byte) {
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
	var memory int64
	if oom {
		if p.native.unenforced {
			t.Fatal("Docker memory enforcement is required for OOM acceptance")
		}
		memory = 32 << 20
	}
	ls := []compute.Launch{{Request: compute.Request{LaunchID: id, Allocation: compute.Allocation{Image: "alpine:3.21", Platform: platform, MemoryBytes: memory}, Metadata: map[string]string{"pulse_job_id": id}}, Process: compute.Process{Command: []string{"/bin/sh"}, Args: []string{"-c", script}}}}
	out, err := p.native.Submit(ctx, ls)
	p.capture.admitted(id)
	if err != nil || len(out) != 1 || out[0].Status != compute.Accepted {
		t.Fatal(out, err)
	}
	history, err := p.capture.store.Histories()
	if err != nil || len(history) != 1 || history[0].LaunchID != id || history[0].ContainerID == "" {
		t.Fatalf("missing exact fast-capture container identity: history=%+v error=%v", history, err)
	}
	containerID := history[0].ContainerID
	page := waitFinalLog(t, ctx, p, id)
	// Docker multiplexes independent pipes; their frame order can differ from
	// the shell's write order. Require byte fidelity and labels within each stream.
	streams := map[api.LogSource][]byte{}
	for _, r := range page.Records {
		streams[r.Source] = append(streams[r.Source], r.Data...)
	}
	if string(streams[api.LogSourceStdout]) != string(stdout) || string(streams[api.LogSourceStderr]) != string(stderr) || !page.Finalized || page.Incomplete {
		t.Fatal("fast output changed", streams, page)
	}
	process := assertCaptureOutcome(t, p, id, code)
	if process.OOMKilled == nil || *process.OOMKilled != oom || !strings.Contains(string(streams[api.LogSourceLaunch]), fmt.Sprintf("Docker worker exited: code=%d, oom_killed=%v", code, oom)) {
		t.Fatal("missing Docker OOM evidence or launch summary", process, streams[api.LogSourceLaunch])
	}
	assertCaptureCleanup(t, ctx, cfg, containerID)
	p.Close()
	p, err = NewWithOptions(ctx, cfg, Options{Logging: &LoggingConfig{Directory: directory, Owner: "fixture", Compress: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if again, err := p.GetLaunchLog(ctx, api.LaunchLogRequest{LaunchID: id}); err != nil || !reflect.DeepEqual(again.Records, page.Records) || !again.Finalized || again.Incomplete {
		t.Fatal(again, err)
	}
	assertCaptureOutcome(t, p, id, code)
	t.Logf("container=%s stdout=%x stderr=%x", containerID, sha256.Sum256(stdout), sha256.Sum256(stderr))
}

func assertCaptureOutcome(t *testing.T, p *Provider, id string, code int) *launchlogs.Process {
	t.Helper()
	obs, err := p.capture.store.Observation(id)
	process := obs.History.Process
	if err != nil || process == nil || process.State != "exited" || process.ExitCode == nil || *process.ExitCode != code {
		t.Fatal("missing Docker observation", obs.History, err)
	}
	t.Logf("launch=%s container=%s exit=%d source=%s", id, obs.History.ContainerID, code, process.Source)
	return process
}

func assertCaptureCleanup(t *testing.T, ctx context.Context, cfg Config, containerID string) {
	t.Helper()
	removal, cancel := context.WithTimeout(ctx, 15*time.Second)
	ids, err := waitRemoteFixtureRemoval(removal, func(check context.Context) (string, error) {
		b, err := exec.CommandContext(check, "docker", "--host", cfg.Socket, "ps", "-aq", "--no-trunc", "--filter", "id="+containerID).CombinedOutput()
		if err != nil {
			return "", fmt.Errorf("query worker removal: %w: %s", err, b)
		}
		return string(b), nil
	})
	cancel()
	if err != nil {
		diagnostic, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		state, inspectErr := exec.CommandContext(diagnostic, "docker", "--host", cfg.Socket, "inspect", "--format", "status={{.State.Status}} exit={{.State.ExitCode}} autoremove={{.HostConfig.AutoRemove}}", containerID).CombinedOutput()
		t.Fatalf("provider did not remove stopped worker: %v; last inventory=%q; inspect error=%v; state=%s", err, ids, inspectErr, state)
	}
	t.Logf("provider removed container %s before fixture teardown", containerID)
}
func TestDockerCaptureRestartIntegration(t *testing.T) {
	for _, scenario := range []string{"running_reopen", "stopped_reopen", "forced_removal", "rotation", "grace_expiry"} {
		t.Run(scenario, func(t *testing.T) { testDockerCaptureRestart(t, scenario) })
	}
}

func testDockerCaptureRestart(t *testing.T, scenario string) {
	ctx, cfg, docker := liveDocker(t)
	platform := strings.ReplaceAll(strings.ReplaceAll(docker("info", "--format", "{{.OSType}}/{{.Architecture}}"), "aarch64", "arm64"), "x86_64", "amd64")
	directory := t.TempDir()
	options := Options{Logging: &LoggingConfig{Directory: directory, Owner: "fixture"}}
	p, err := NewWithOptions(ctx, cfg, options)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	id := fmt.Sprintf("restart-%d", time.Now().UnixNano())
	p.capture.store.Begin(launchHistory(id))
	script := "echo prefix; while [ ! -f /tmp/release ]; do sleep .1; done; echo suffix; echo separate-stderr >&2; exit 7"
	if scenario == "rotation" {
		script = "echo prefix; while [ ! -f /tmp/release ]; do sleep .1; done; yes rotation-line | head -c 40000000; exit 7"
	}
	l := compute.Launch{Request: compute.Request{LaunchID: id, Allocation: compute.Allocation{Image: "alpine:3.21", Platform: platform}, Metadata: map[string]string{"pulse_job_id": id}}, Process: compute.Process{Command: []string{"/bin/sh"}, Args: []string{"-c", script}}}
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
	now := time.Now
	switch scenario {
	case "forced_removal":
		docker("rm", "-f", container)
	case "running_reopen":
		if state := docker("inspect", "--format", "{{.State.Status}}", container); state != "running" {
			t.Fatal(state)
		}
	default:
		docker("exec", container, "touch", "/tmp/release")
		if code := docker("wait", container); code != "7" {
			t.Fatal(code)
		}
		if stopped := docker("inspect", "--format", "{{.State.Status}} {{.State.ExitCode}}", container); stopped != "exited 7" {
			t.Fatal(stopped)
		}
		t.Logf("container=%s stopped during downtime, exit=7", container)
		if scenario == "grace_expiry" {
			now = func() time.Time { return time.Now().Add(25 * time.Hour) }
		}
	}
	p, err = newWithOptions(ctx, cfg, options, now)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if scenario == "running_reopen" {
		docker("exec", container, "touch", "/tmp/release")
	}
	page := waitFinalLog(t, ctx, p, id)
	if !page.Incomplete {
		t.Fatal("downtime removal not disclosed")
	}
	var stdout, stderr string
	for _, r := range page.Records {
		switch r.Source {
		case api.LogSourceStdout:
			stdout += string(r.Data)
		case api.LogSourceStderr:
			stderr += string(r.Data)
		}
	}
	observation, err := p.capture.store.Observation(id)
	if err != nil {
		t.Fatal(err)
	}
	if scenario == "forced_removal" {
		process := observation.History.Process
		if process == nil || process.State != "unknown" || process.ExitCode != nil || process.Reason != "container_missing" || !slices.Contains(observation.History.GapReasons, "container_missing") {
			t.Fatal(observation)
		}
	} else {
		assertCaptureOutcome(t, p, id, 7)
		assertCaptureCleanup(t, ctx, cfg, container)
	}
	switch scenario {
	case "running_reopen", "stopped_reopen":
		if stdout != "prefix\nsuffix\n" || stderr != "separate-stderr\n" {
			t.Fatal("missing or duplicate recovered bytes", stdout, stderr)
		}
	case "rotation", "grace_expiry", "forced_removal":
		reason := map[string]string{"rotation": "replay_unverifiable", "grace_expiry": "native_retention_expired", "forced_removal": "container_missing"}[scenario]
		if stdout != "prefix\n" || stderr != "" || !slices.Contains(observation.History.GapReasons, reason) {
			t.Fatal("unknown replay boundary", stdout, stderr, observation)
		}
	}
	if scenario == "grace_expiry" {
		again, err := p.native.Submit(ctx, []compute.Launch{l})
		if err != nil || len(again) != 1 || again[0].Status != compute.Accepted {
			t.Fatal("accepted identity not preserved", again, err)
		}
		assertCaptureCleanup(t, ctx, cfg, container)
		if newID := docker("ps", "-aq", "--filter", "label=pulse_launch_id="+id); newID != "" {
			t.Fatal("receipt allowed replay", newID)
		}
	}
	t.Logf("container=%s stdout=%x stderr=%x gaps=%v", container, sha256.Sum256([]byte(stdout)), sha256.Sum256([]byte(stderr)), observation.History.GapReasons)
}
func TestDockerManagementIntegration(t *testing.T) {
	ctx, cfg, docker := liveDocker(t)
	binary := remoteBinary(t)
	cfg.DependencyCache = CacheConfig{Namespace: fmt.Sprintf("refresh-%d", time.Now().UnixNano()), Generation: "fixture", MaxAge: time.Hour}
	logging := LoggingConfig{Directory: t.TempDir(), Owner: "management-fixture", Compress: true}
	p, err := NewWithOptions(ctx, cfg, Options{Logging: &logging})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	original := docker("image", "inspect", "--format", "{{.Id}}", bases.Image)
	id := fmt.Sprintf("refresh-worker-%d", time.Now().UnixNano())
	completedID := id + "-completed"
	platform := p.platforms[0]
	l := compute.Launch{Request: compute.Request{LaunchID: id, Allocation: compute.Allocation{Image: bases.Image, Platform: platform, MemoryBytes: 256 << 20}, Metadata: map[string]string{"pulse_jobdb_instance_id": "fixture", "pulse_tenant_id": "fixture", "pulse_job_id": id}}, Process: compute.Process{Command: []string{"/bin/sh"}, Args: []string{"-c", "echo refresh-marker > /var/cache/pulse/refresh-marker; sleep 300"}}}
	t.Cleanup(func() {
		for _, launchID := range []string{id, completedID} {
			ids, _ := exec.Command("docker", "--host", cfg.Socket, "ps", "-aq", "--filter", "label=pulse_launch_id="+launchID).Output()
			for _, container := range strings.Fields(string(ids)) {
				exec.Command("docker", "--host", cfg.Socket, "rm", "-f", container).Run()
			}
		}
		volumes, _ := exec.Command("docker", "--host", cfg.Socket, "volume", "ls", "-q", "--filter", "label=pulse_cache_owner="+p.native.cacheOwner()).Output()
		for _, volume := range strings.Fields(string(volumes)) {
			exec.Command("docker", "--host", cfg.Socket, "volume", "rm", volume).Run()
		}
	})
	start := func(t *testing.T, launch compute.Launch) {
		t.Helper()
		if err := p.capture.store.Begin(launchHistory(launch.LaunchID)); err != nil {
			t.Fatal(err)
		}
		out, err := p.native.Submit(ctx, []compute.Launch{launch})
		if err != nil || len(out) != 1 || out[0].Status != compute.Accepted {
			t.Fatal(out, err)
		}
		if err := p.capture.store.RecordAdmission(launch.LaunchID, string(api.Accepted), false, false); err != nil {
			t.Fatal(err)
		}
		p.capture.admitted(launch.LaunchID)
	}
	start(t, l)
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
	t.Run("read_launch_logs_beside_active_owner", func(t *testing.T) {
		completed := l
		completed.LaunchID = completedID
		completed.Metadata = map[string]string{"pulse_jobdb_instance_id": "fixture", "pulse_tenant_id": "fixture", "pulse_job_id": completedID}
		completed.Process.Args = []string{"-c", "echo worker-stdout; echo worker-stderr >&2; exit 7"}
		start(t, completed)
		waitFinalLog(t, ctx, p, completedID)
		assertCaptureOutcome(t, p, completedID, 7)
		before, err := p.capture.store.DurableObservation(completedID)
		if err != nil {
			t.Fatal(err)
		}
		assertCaptureCleanup(t, ctx, cfg, before.History.ContainerID)
		before, err = p.capture.store.DurableObservation(completedID)
		if err != nil {
			t.Fatal(err)
		}
		active := docker("inspect", "--format", "{{.Id}} {{.State.Status}} {{.State.StartedAt}}", container)
		if !strings.Contains(active, " running ") {
			t.Fatal("owner has no active worker", active)
		}
		cli := func(arguments ...string) (string, string, error) {
			args := append(arguments, "--socket", cfg.Socket, "--lock-dir", cfg.LockDir, "--log-directory", logging.Directory, "--owner", logging.Owner)
			cmd := exec.CommandContext(ctx, binary, args...)
			var diagnostic bytes.Buffer
			cmd.Stderr = &diagnostic
			output, err := cmd.Output()
			return string(output), diagnostic.String(), err
		}
		output, diagnostic, err := cli("launches", "show", completedID, "--json")
		var history launchlogs.History
		if err != nil || json.Unmarshal([]byte(output), &history) != nil || history.LaunchID != completedID || history.Admission != string(api.Accepted) || history.Capture != "closed" || history.Incomplete || history.Process == nil || history.Process.ExitCode == nil || *history.Process.ExitCode != 7 {
			t.Fatal(output, diagnostic, err)
		}
		output, diagnostic, err = cli("logs", completedID, "--stream", "stderr")
		if err != nil || output != "worker-stderr\n" || !strings.Contains(diagnostic, "Docker worker exited: code=7") || strings.Contains(diagnostic, "worker-stderr") || strings.Contains(diagnostic, "worker-stdout") {
			t.Fatal(output, diagnostic, err)
		}
		after, err := p.capture.store.DurableObservation(completedID)
		if err != nil || !reflect.DeepEqual(after, before) {
			t.Fatal("management changed the archived launch", before, after, err)
		}
		if got := docker("inspect", "--format", "{{.Id}} {{.State.Status}} {{.State.StartedAt}}", container); got != active {
			t.Fatal("management changed the holding worker", active, got)
		}
		if got := docker("ps", "-aq", "--filter", "label=pulse_launch_id="+completedID); got != "" {
			t.Fatal("management recreated the completed worker", got)
		}
	})
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
