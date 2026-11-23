//go:build integration

package provider

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/colony-2/c2j/pkg/contextual"
	"github.com/colony-2/c2j/pkg/jobdbschema"
	"github.com/colony-2/c2j/pkg/jobref"
	"github.com/colony-2/c2j/pkg/ops"
	"github.com/colony-2/c2j/pkg/recipe"
	"github.com/colony-2/c2j/pkg/starter"
	"github.com/colony-2/c2j/pkg/worker/commandop"
	"github.com/colony-2/c2j/pkg/workflowctl"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	jobworkflow "github.com/colony-2/jobdb/pkg/workflow"
	api "github.com/colony-2/pulse-provider-api"
	"github.com/colony-2/pulse-provider-api/plugin"
)

type cliFaultListener struct {
	net.Listener
	mu     sync.Mutex
	latest net.Conn
}

func (l *cliFaultListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		l.mu.Lock()
		l.latest = c
		l.mu.Unlock()
	}
	return c, err
}
func (l *cliFaultListener) disconnect() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.latest != nil {
		_ = l.latest.Close()
	}
}
func waitCLIProvider(t *testing.T, ctx context.Context, c *plugin.Collector, after uint64) plugin.AvailableProvider {
	t.Helper()
	for {
		snapshot, err := c.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range snapshot.Providers {
			if p.Generation > after {
				return p
			}
		}
		if _, err := c.WaitForChange(ctx, snapshot.Revision); err != nil {
			t.Fatal(err)
		}
	}
}
func waitCLICondition(t *testing.T, ctx context.Context, condition func() bool) {
	t.Helper()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for !condition() {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
}

type cliProcess struct {
	cmd    *exec.Cmd
	done   chan error
	waited bool
	logs   *os.File
}

func startCLI(t *testing.T, ctx context.Context, binary string, args []string, key string) *cliProcess {
	t.Helper()
	logs, err := os.CreateTemp(t.TempDir(), "provider-*.log")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = append(os.Environ(), "HOME="+t.TempDir(), "PULSE_CLI_LIVE_KEY="+key)
	cmd.Stdout = logs
	cmd.Stderr = logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &cliProcess{cmd: cmd, done: make(chan error, 1), logs: logs}
	go func() { p.done <- cmd.Wait() }()
	t.Cleanup(func() {
		if !p.waited {
			_ = p.cmd.Process.Kill()
			<-p.done
		}
		_ = logs.Close()
		if t.Failed() {
			data, _ := os.ReadFile(logs.Name())
			t.Logf("provider diagnostics:\n%s", data)
		}
	})
	return p
}
func (p *cliProcess) stop(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-p.done:
		p.waited = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("provider exceeded shutdown bound")
	}
}

func newCLIJob(t *testing.T, ctx context.Context, f *dockerJobDB, base, scenario string) (api.Launch, jobref.Ref, jobdb.ExecutionLease) {
	t.Helper()
	run := pythonHTTPGet(f.url+"/gate") + "; test \"$(cat README)\" = fixture; echo external-container-result"
	if scenario == "expired" || scenario == "completed" {
		run = pythonHTTPGet(f.url + "/forbidden")
	}
	rec, err := recipe.LoadRecipeFromString([]byte("id: external-docker\nexecution:\n  base: '" + base + "'\nop: command_execution\ninputs:\n  run: " + fmt.Sprintf("%q", "set -eu; "+run) + "\noutputs:\n  result: '${{ op.outputs.stdout }}'\n"))
	if err != nil {
		t.Fatal(err)
	}
	engine, err := jobworkflow.NewEngineBuilder().WithRuntime(f.backend).BuildEngine()
	if err != nil {
		t.Fatal(err)
	}
	start := func() jobref.Ref {
		ref, err := starter.StartRecipeJob(ctx, workflowctl.StartJob{Cell: "test", CellRepo: "example.test/pulse-fixture", TenantId: "docker-test", RecipeName: rec.GetMetadata().ID, GitRef: f.hash, JobContext: contextual.JobContext{Workflow: contextual.WorkflowContext{ProjectId: "docker-test", CellName: "."}, GitBase: contextual.GitBaseContext{BaseRepo: f.url + "/git/repo.git", BaseRef: f.hash, ResolvedBaseHash: f.hash}}}, jobdbschema.WorkflowEngine{Engine: engine, Registry: f.backend}, *rec)
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}
	ref, unrelated := start(), start()
	unrelatedBefore, err := f.backend.GetJob(ctx, unrelated.Key())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			return
		}
		after, err := f.backend.GetJob(context.Background(), unrelated.Key())
		if err != nil || after.Status != unrelatedBefore.Status {
			t.Error("unassigned job changed", err)
		}
	})
	duration := time.Minute
	if scenario == "expired" {
		duration = time.Second
	}
	lease, err := f.backend.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: ref.Key(), WorkerID: "external-docker-test", Routes: []jobdb.Route{{JobType: "recipe"}}, LeaseDuration: duration})
	if err != nil || lease == nil {
		t.Fatal("acquire fixture lease", err)
	}
	exported, err := remote.ExportLease(lease)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := exported.Encode()
	if err != nil {
		t.Fatal(err)
	}
	launch := api.Launch{LaunchID: ref.Key().JobId, Allocation: api.Allocation{Base: base}, Job: api.JobRef{JobDB: f.url + "/docker-test", InstanceID: "fixture", TenantID: "docker-test", JobID: ref.Key().JobId}, Lease: api.Lease(encoded)}
	return launch, ref, lease
}
func submitCLI(t *testing.T, ctx context.Context, p api.Provider, l api.Launch) {
	t.Helper()
	results, sub, err := p.Submit(ctx, []api.Launch{l}, api.SubmitOptions{})
	if err != nil || sub != nil || len(results) != 1 || results[0].LaunchID != l.LaunchID || results[0].Status != api.Accepted {
		t.Fatalf("external admission: results=%v error=%v", results, err)
	}
}
func assertCLIListing(t *testing.T, ctx context.Context, p api.Provider, l api.Launch, id string) {
	t.Helper()
	list, err := p.List(ctx, api.ListRequest{LaunchID: l.LaunchID, PageSize: 1})
	if err != nil || len(list.Items) != 1 || list.Items[0].LaunchID != l.LaunchID || list.Items[0].Job != l.Job || list.Items[0].ID != id {
		t.Fatalf("external listing: %+v %v", list, err)
	}
}

// TestDockerCLIIntegration exercises the production executable; no test opener
// or special production mode exists. Missing prerequisites fail this acceptance.
func TestDockerCLIIntegration(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Fatal("CLI live acceptance requires Linux")
	}
	ops.Register(commandop.GetOp())
	socket, err := ResolveSocket("")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Minute)
	defer cancel()
	docker := func(t *testing.T, args ...string) string {
		t.Helper()
		out, err := exec.CommandContext(ctx, "docker", append([]string{"--host", socket}, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("docker %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	platform := strings.ReplaceAll(strings.ReplaceAll(docker(t, "info", "--format", "{{.OSType}}/{{.Architecture}}"), "aarch64", "arm64"), "x86_64", "amd64")
	if platform != "linux/arm64" && platform != "linux/amd64" {
		t.Fatal("unsupported daemon platform", platform)
	}
	docker(t, "pull", "--platform", platform, c2jIntegrationImage)
	t.Log("daemon", docker(t, "version"))
	t.Log("image", docker(t, "image", "inspect", "--format", "{{json .RepoDigests}}", c2jIntegrationImage))
	t.Log(docker(t, "run", "--rm", "--platform", platform, "--entrypoint", "c2j", c2jIntegrationImage, "version"))
	binary := filepath.Join(t.TempDir(), "pulse-docker")
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, "../../cmd/pulse-docker")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v\n%s", err, out)
	}
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("production binary sha256=%x", sha256.Sum256(data))
	fixture := buildDockerJobDBFixture(t, ctx, docker)
	base := "ubuntu-26.04-" + strings.TrimPrefix(platform, "linux/")
	for _, scenario := range []string{"outbound_restart", "inbound_parent_loss", "invalid_leases"} {
		if !t.Run(scenario, func(t *testing.T) {
			if scenario == "invalid_leases" {
				for _, invalid := range []string{"expired", "completed"} {
					t.Run(invalid, func(t *testing.T) { testCLIInvalidLease(t, ctx, docker, socket, binary, fixture, base, invalid) })
				}
				return
			}
			sessionCtx, stop := context.WithTimeout(ctx, 8*time.Minute)
			defer stop()
			f := newDockerJobDB(t, sessionCtx, docker, socket, fixture)
			f.control(t, "hold-gate")
			launch, ref, _ := newCLIJob(t, sessionCtx, f, base, scenario)
			lockDir := t.TempDir()
			common := []string{"server", "--owner", "fixture", "--socket", socket, "--lock-dir", lockDir, "--cache-disabled", "--shutdown-timeout", "10s", "--startup-timeout", "10m", "--log-directory", t.TempDir()}
			// Register fixture-scoped cleanup before any possible ambiguous admission.
			t.Cleanup(func() {
				cleanup, done := context.WithTimeout(context.Background(), time.Minute)
				defer done()
				out, _ := exec.CommandContext(cleanup, "docker", "--host", socket, "ps", "-aq", "--no-trunc", "--filter", "label=pulse_job_id="+launch.Job.JobID).CombinedOutput()
				for _, id := range strings.Fields(string(out)) {
					_ = exec.CommandContext(cleanup, "docker", "--host", socket, "rm", "-f", id).Run()
				}
			})
			var p api.Provider
			var outbound *cliProcess
			var collector *plugin.Collector
			var listener *cliFaultListener
			var first plugin.AvailableProvider
			var parent *exec.Cmd
			var parentDone chan error
			parentWaited := false
			var args []string
			if scenario == "outbound_restart" {
				raw, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				listener = &cliFaultListener{Listener: raw}
				collector, err = plugin.StartCollector(sessionCtx, listener, plugin.CollectorOptions{AuthMode: plugin.ReverseAuthMutualKey, Transport: plugin.ReverseTransportHTTP, MutualKeys: map[string][]byte{"worker": []byte("live-fixture-key")}})
				if err != nil {
					t.Fatal(err)
				}
				defer collector.Close()
				args = append(append([]string{}, common...), "--collector", raw.Addr().String(), "--name", "worker", "--reverse-auth", "mutual-key", "--reverse-transport", "http", "--shared-key-env", "PULSE_CLI_LIVE_KEY", "--reconnect-initial-delay", "10ms", "--reconnect-max-delay", "100ms")
				outbound = startCLI(t, sessionCtx, binary, args, "live-fixture-key")
				first = waitCLIProvider(t, sessionCtx, collector, 0)
				p = first.Provider
				submitCLI(t, sessionCtx, p, launch)
			} else {
				dir := t.TempDir()
				request := filepath.Join(dir, "request.json")
				ready := filepath.Join(dir, "ready.json")
				payload, _ := json.Marshal(cliHostRequest{Binary: binary, Args: append(append([]string{}, common...), "--mode", "inbound"), BaseDir: t.TempDir(), Launch: launch, Ready: ready})
				if err := os.WriteFile(request, payload, 0600); err != nil {
					t.Fatal(err)
				}
				executable, err := os.Executable()
				if err != nil {
					t.Fatal(err)
				}
				parent = exec.CommandContext(sessionCtx, executable, "-test.run=^TestDockerCLIHostProcess$")
				parent.Env = append(os.Environ(), "PULSE_CLI_HOST_REQUEST="+request, "HOME="+t.TempDir())
				parentDone = make(chan error, 1)
				if err := parent.Start(); err != nil {
					t.Fatal(err)
				}
				go func() { parentDone <- parent.Wait() }()
				defer func() {
					if !parentWaited {
						_ = parent.Process.Kill()
						<-parentDone
					}
				}()
				waitCLICondition(t, sessionCtx, func() bool { _, err := os.Stat(ready); return err == nil })
			}
			ids := strings.Fields(docker(t, "ps", "-aq", "--no-trunc", "--filter", "label=pulse_job_id="+launch.Job.JobID))
			if len(ids) != 1 {
				t.Fatal("expected exactly one worker", ids)
			}
			id := ids[0]
			observation := observeDocker(t, sessionCtx, socket, id)
			f.control(t, "observed")
			waitCLICondition(t, sessionCtx, func() bool { return f.stats(t).Renewals >= 3 })
			if p != nil {
				assertCLIListing(t, sessionCtx, p, launch, id)
			}
			// A different owner cannot bypass the same daemon lock.
			contender, err := plugin.Start(sessionCtx, plugin.StartOptions{Executable: binary, Args: append(append([]string{}, common...), "--mode", "inbound", "--owner", "other"), Env: map[string]string{"HOME": t.TempDir()}, Init: plugin.Initialization{Name: "other", BaseDir: t.TempDir()}, StartupTimeout: 10 * time.Second})
			if err == nil {
				_ = contender.Close()
				t.Fatal("second owner acquired active daemon")
			}
			renewals := f.stats(t).Renewals
			if scenario == "outbound_restart" {
				listener.disconnect()
				second := waitCLIProvider(t, sessionCtx, collector, first.Generation)
				assertCLIListing(t, sessionCtx, second.Provider, launch, id)
				outbound.stop(t)
				waitCLICondition(t, sessionCtx, func() bool { return f.stats(t).Renewals > renewals })
				renewals = f.stats(t).Renewals
				outbound = startCLI(t, sessionCtx, binary, args, "live-fixture-key")
				third := waitCLIProvider(t, sessionCtx, collector, second.Generation)
				assertCLIListing(t, sessionCtx, third.Provider, launch, id)
				outbound.stop(t)
			} else {
				if err := parent.Process.Kill(); err != nil {
					t.Fatal(err)
				}
				<-parentDone
				parentWaited = true
				waitCLICondition(t, sessionCtx, func() bool { return f.stats(t).Renewals > renewals })
				renewals = f.stats(t).Renewals
				var child *plugin.Process
				waitCLICondition(t, sessionCtx, func() bool {
					var err error
					child, err = plugin.Start(sessionCtx, plugin.StartOptions{Executable: binary, Args: append(append([]string{}, common...), "--mode", "inbound"), Env: map[string]string{"HOME": t.TempDir()}, Init: plugin.Initialization{Name: "fixture", BaseDir: t.TempDir()}, StartupTimeout: 5 * time.Second})
					return err == nil
				})
				assertCLIListing(t, sessionCtx, child, launch, id)
				if err := child.Close(); err != nil {
					t.Fatal(err)
				}
			}
			waitCLICondition(t, sessionCtx, func() bool { return f.stats(t).Renewals > renewals })
			if current := docker(t, "ps", "-aq", "--no-trunc", "--filter", "label=pulse_job_id="+launch.Job.JobID); current != id {
				t.Fatal("worker duplicated or replaced", current)
			}
			if f.stats(t).Claims != 1 {
				t.Fatal("transport reconnect reacquired work")
			}
			f.control(t, "release-gate")
			code, logs := observation.finish(t, sessionCtx)
			if code != "0" {
				t.Fatalf("worker exit=%s logs=%s", code, logs)
			}
			info, err := f.backend.GetJob(sessionCtx, ref.Key())
			if err != nil {
				t.Fatal(err)
			}
			result, resultErr := info.Data.GetData()
			if info.Status != jobdb.JobStatusCompleted || resultErr != nil || !strings.Contains(string(result), "external-container-result") {
				t.Fatal("missing durable external result", info.Status, resultErr)
			}
			if strings.Contains(logs, string(launch.Lease)) {
				t.Fatal("lease leaked into logs")
			}
			t.Logf("worker=%s launch=%s final_job_status=%s claims=%d renewals=%d", id, launch.LaunchID, info.Status, f.stats(t).Claims, f.stats(t).Renewals)
		}) {
			t.Fatal("external acceptance failed; stopping scenarios")
		}
	}
}

type cliHostRequest struct {
	Binary  string
	Args    []string
	BaseDir string
	Launch  api.Launch
	Ready   string
}

// This is a separate SDK host process, not a provider test mode. Killing it
// exercises real parent loss of the production inbound executable.
func TestDockerCLIHostProcess(t *testing.T) {
	request := os.Getenv("PULSE_CLI_HOST_REQUEST")
	if request == "" {
		return
	}
	data, err := os.ReadFile(request)
	if err != nil {
		t.Fatal(err)
	}
	var in cliHostRequest
	if err := json.Unmarshal(data, &in); err != nil {
		t.Fatal(err)
	}
	process, err := plugin.Start(t.Context(), plugin.StartOptions{Executable: in.Binary, Args: in.Args, Env: map[string]string{"HOME": os.Getenv("HOME")}, Init: plugin.Initialization{Name: "fixture", BaseDir: in.BaseDir}})
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	submitCLI(t, t.Context(), process, in.Launch)
	list, err := process.List(t.Context(), api.ListRequest{LaunchID: in.Launch.LaunchID})
	if err != nil || len(list.Items) != 1 {
		t.Fatal("host listing failed", err)
	}
	if err := os.WriteFile(in.Ready, []byte("ready"), 0600); err != nil {
		t.Fatal(err)
	}
	<-t.Context().Done()
}
func testCLIInvalidLease(t *testing.T, ctx context.Context, docker func(*testing.T, ...string) string, socket, binary, fixture, base, invalid string) {
	f := newDockerJobDB(t, ctx, docker, socket, fixture)
	launch, ref, lease := newCLIJob(t, ctx, f, base, invalid)
	if invalid == "completed" {
		if err := completeDockerFixtureLease(ctx, f.backend, lease); err != nil {
			t.Fatal(err)
		}
	} else {
		expiry, ok := lease.(interface{ LeaseExpiry() time.Time })
		if !ok {
			t.Fatal("fixture has no expiry")
		}
		select {
		case <-time.After(max(0, time.Until(expiry.LeaseExpiry())) + time.Second):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	before, err := f.backend.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: ref.Key()})
	if err != nil {
		t.Fatal(err)
	}
	process, err := plugin.Start(ctx, plugin.StartOptions{Executable: binary, Args: []string{"server", "--owner", "fixture", "--mode", "inbound", "--socket", socket, "--lock-dir", t.TempDir(), "--cache-disabled", "--log-directory", t.TempDir(), "--startup-timeout", "10m"}, Env: map[string]string{"HOME": t.TempDir()}, Init: plugin.Initialization{Name: "fixture", BaseDir: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	submitCLI(t, ctx, process, launch)
	ids := strings.Fields(docker(t, "ps", "-aq", "--no-trunc", "--filter", "label=pulse_job_id="+launch.Job.JobID))
	if len(ids) != 1 {
		t.Fatal("missing invalid lease worker", ids)
	}
	observation := observeDocker(t, ctx, socket, ids[0])
	f.control(t, "observed")
	code, logs := observation.finish(t, ctx)
	after, err := f.backend.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: ref.Key()})
	if err != nil || len(after) != len(before) || code == "0" || f.stats(t).Forbidden != 0 || !strings.Contains(logs, "job execution has not started") {
		t.Fatalf("invalid lease executed recipe: exit=%s chapters=%d error=%v", code, len(after), err)
	}
	t.Logf("%s lease: c2j rejected execution; provider admission remained separate", invalid)
}
