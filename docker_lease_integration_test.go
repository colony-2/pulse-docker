//go:build integration

package docker

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"

	"os/exec"

	"strings"
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
	provider "github.com/colony-2/pulse-provider-api"
)

const c2jIntegrationImage = "ghcr.io/colony-2/base2:latest"

// TestDockerLeaseIntegration exercises the installed c2j with an explicitly
// acquired fixture lease. No controller or scheduler participates.
func TestDockerLeaseIntegration(t *testing.T) {
	ops.Register(commandop.GetOp())
	socket, err := ResolveSocket("")
	if err != nil {
		t.Fatal(err)
	}
	// A cold pull includes downloading and unpacking a large development image.
	// Give preparation its own budget so slow registry/VM I/O cannot consume
	// the time reserved for executing the actual jobs.
	pullCtx, stopPull := context.WithTimeout(t.Context(), 30*time.Minute)
	started := time.Now()
	t.Logf("pulling %s (image preparation timeout: 30m)", c2jIntegrationImage)
	platformOut, err := exec.CommandContext(pullCtx, "docker", "--host", socket, "info", "--format", "{{.OSType}}/{{.Architecture}}").CombinedOutput()
	if err != nil {
		stopPull()
		t.Fatalf("daemon platform: %v: %s", err, platformOut)
	}
	platform := strings.TrimSpace(string(platformOut))
	platform = strings.ReplaceAll(strings.ReplaceAll(platform, "aarch64", "arm64"), "x86_64", "amd64")
	if platform != "linux/arm64" && platform != "linux/amd64" {
		stopPull()
		t.Fatal("unsupported fixture platform", platform)
	}
	baseName := "ubuntu-26.04-" + strings.TrimPrefix(platform, "linux/")
	pullOutput, pullErr := exec.CommandContext(pullCtx, "docker", "--host", socket, "pull", "--platform", platform, c2jIntegrationImage).CombinedOutput()
	pullContextErr := pullCtx.Err()
	stopPull()
	if pullErr != nil {
		t.Fatalf("prepare executor image after %s: %v (context: %v)\n%s", time.Since(started), pullErr, pullContextErr, pullOutput)
	}
	t.Logf("executor image ready after %s; starting integration scenarios (8m budget each)", time.Since(started))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	dockerFor := func(ctx context.Context) func(*testing.T, ...string) string {
		return func(t *testing.T, args ...string) string {
			t.Helper()
			out, err := exec.CommandContext(ctx, "docker", append([]string{"--host", socket}, args...)...).CombinedOutput()
			if err != nil {
				t.Fatalf("docker %v: %v\n%s", args, err, out)
			}
			return strings.TrimSpace(string(out))
		}
	}
	docker := dockerFor(ctx)
	// The provider pins the locally addressable root ID and selects ARM64 at
	// creation; containerd's platform-specific child ID may not be addressable.
	imageID := docker(t, "image", "inspect", "--format", "{{.Id}}", c2jIntegrationImage)
	t.Logf("executor image: %s", docker(t, "image", "inspect", "--format", "{{json .RepoDigests}}", c2jIntegrationImage))
	t.Log(docker(t, "run", "--rm", "--platform", platform, "--entrypoint", "c2j", c2jIntegrationImage, "version"))

	fixtureBinary := buildDockerJobDBFixture(t, ctx, docker)

	for _, scenario := range []string{"success and heartbeat", "peer connectivity", "host gateway connectivity", "recipe failure", "recipe timeout", "lease import transport failure", "heartbeat transport failure", "expired lease", "completed lease"} {
		if !t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
			defer cancel()
			docker := dockerFor(ctx)
			f := newDockerJobDB(t, ctx, docker, socket, fixtureBinary)
			run := "test \"$(cat README)\" = fixture; echo saved > result.txt; echo container-result"
			extra := ""
			switch scenario {
			case "success and heartbeat":
				// Wait for a real background renewal before allowing the command to
				// complete. There is no sleep-based assumption about VM startup speed.
				run = pythonHTTPGet(f.url+"/gate") + "; " + run
			case "peer connectivity":
				run = "test \"$(" + pythonHTTPGet(f.url+"/healthz") + ")\" = pulse-jobdb-fixture; " + run
			case "host gateway connectivity":
				_, port, err := net.SplitHostPort(strings.TrimPrefix(f.controllerURL, "http://"))
				if err != nil {
					t.Fatal(err)
				}
				gateway := "http://" + net.JoinHostPort("host.docker.internal", port)
				run = "test \"$(" + pythonHTTPGet(gateway+"/healthz") + ")\" = pulse-jobdb-fixture; " + run
			case "expired lease", "completed lease":
				run = pythonHTTPGet(f.url + "/forbidden")
			case "recipe failure":
				run = "echo intentional-recipe-failure >&2; exit 23"
			case "recipe timeout":
				extra = "timeout: 1s\n"
				run = "sleep 300; echo must-not-complete"
			case "lease import transport failure":
				f.control(t, "drop-import")
				run = pythonHTTPGet(f.url + "/forbidden")
			case "heartbeat transport failure":
				f.control(t, "drop-heartbeat")
				run = pythonHTTPGet(f.url+"/gate") + "; " + pythonHTTPGet(f.url+"/forbidden")
			}
			run = "set -eu; " + run
			rec, err := recipe.LoadRecipeFromString([]byte("id: docker-executor\nexecution:\n  base: '" + baseName + "'\n" + extra + "op: command_execution\ninputs:\n  run: " + fmt.Sprintf("%q", run) + "\noutputs:\n  result: '${{ op.outputs.stdout }}'\n"))
			if err != nil {
				t.Fatal(err)
			}
			engine, err := jobworkflow.NewEngineBuilder().WithRuntime(f.backend).BuildEngine()
			if err != nil {
				t.Fatal(err)
			}
			startJob := func() (jobref.Ref, error) {
				return starter.StartRecipeJob(ctx, workflowctl.StartJob{
					Cell: "test", CellRepo: "example.test/pulse-fixture", TenantId: "docker-test", RecipeName: rec.GetMetadata().ID, GitRef: f.hash,
					JobContext: contextual.JobContext{
						Workflow: contextual.WorkflowContext{ProjectId: "docker-test", CellName: "."},
						GitBase:  contextual.GitBaseContext{BaseRepo: f.url + "/git/repo.git", BaseRef: f.hash, ResolvedBaseHash: f.hash},
					},
				}, jobdbschema.WorkflowEngine{Engine: engine, Registry: f.backend}, *rec)
			}
			jobRef, err := startJob()
			if err != nil {
				t.Fatal(err)
			}

			unrelated, err := startJob()
			if err != nil {
				t.Fatal(err)
			}
			unrelatedBefore, err := f.backend.GetJob(ctx, unrelated.Key())
			if err != nil {
				t.Fatal(err)
			}
			key := jobRef.Key()
			duration := time.Minute
			if scenario == "expired lease" {
				duration = time.Second
			}

			p, err := New(ctx, Config{Socket: socket, LockDir: t.TempDir(), DependencyCache: CacheConfig{Disabled: true}})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			lease, err := f.backend.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "docker-library-test", Routes: []jobdb.Route{{JobType: "recipe"}}, LeaseDuration: duration})
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
			if scenario == "completed lease" {
				if err := completeDockerFixtureLease(ctx, f.backend, lease); err != nil {
					t.Fatal(err)
				}
				if info, err := f.backend.GetJob(ctx, key); err != nil || info.Status != jobdb.JobStatusCompleted {
					t.Fatalf("fixture job was not completed before launch: status=%s error=%v", info.Status, err)
				}
			}
			if scenario == "expired lease" {
				expiry, ok := lease.(interface{ LeaseExpiry() time.Time })
				if !ok {
					t.Fatal("fixture lacks lease expiry")
				}
				select {
				case <-time.After(max(0, time.Until(expiry.LeaseExpiry())) + time.Second):
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			chaptersBefore, err := f.backend.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
			if err != nil {
				t.Fatal(err)
			}
			launch := provider.Launch{LaunchID: key.JobId, Allocation: provider.Allocation{Base: baseName}, Job: provider.JobRef{JobDB: f.url + "/docker-test", InstanceID: "fixture", TenantID: "docker-test", JobID: key.JobId}, Lease: provider.Lease(encoded)}
			// Register before launch so an uncertain/failed submission is cleaned up too.
			t.Cleanup(func() {
				cleanup, stop := context.WithTimeout(context.Background(), 2*time.Minute)
				defer stop()
				list, err := exec.CommandContext(cleanup, "docker", "--host", socket, "ps", "-aq", "--filter", "label=pulse_job_id="+key.JobId).CombinedOutput()
				if err != nil {
					t.Errorf("list cleanup containers: %v: %s", err, list)
					return
				}
				for _, id := range strings.Fields(string(list)) {
					if t.Failed() {
						logs, _ := exec.CommandContext(cleanup, "docker", "--host", socket, "logs", id).CombinedOutput()
						t.Logf("c2j container logs:\n%s", logs)
					}
					if output, err := exec.CommandContext(cleanup, "docker", "--host", socket, "rm", "-f", id).CombinedOutput(); err != nil && !strings.Contains(string(output), "No such container") {
						t.Errorf("cleanup: %v\n%s", err, output)
					}
				}

			})
			results, submitErr := p.Submit(ctx, []provider.Launch{launch})
			if submitErr != nil || len(results) != 1 || results[0].Status != provider.Accepted {
				t.Fatal(results, submitErr)
			}
			ids := strings.Fields(docker(t, "ps", "-aq", "--filter", "label=pulse_job_id="+key.JobId))
			if len(ids) != 1 {
				t.Fatalf("expected one launched c2j container, got %v", ids)
			}
			id := ids[0]
			observation := observeDocker(t, ctx, socket, id)
			f.control(t, "observed")
			code, logs := observation.finish(t, ctx)
			t.Logf("c2j container logs:\n%s", logs)
			info, err := f.backend.GetJob(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			data, resultErr := info.Data.GetData()
			t.Logf("c2j exit=%s job=%s claims=%d renewals=%d", code, info.Status, f.stats(t).Claims, f.stats(t).Renewals)
			if f.stats(t).Claims != 1 {
				t.Fatalf("lease was not handed off exactly once: %d acquisitions", f.stats(t).Claims)
			}
			if f.stats(t).Forbidden != 0 {
				t.Fatal("executor continued after losing authority")
			}
			unrelatedAfter, err := f.backend.GetJob(ctx, unrelated.Key())
			if err != nil || unrelatedAfter.Status != unrelatedBefore.Status {
				t.Fatal("unrelated queued work changed", err)
			}
			switch scenario {
			case "expired lease", "completed lease":
				after, err := f.backend.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
				if code == "0" || err != nil || len(after) != len(chaptersBefore) || !strings.Contains(logs, "job execution has not started") {
					t.Fatalf("invalid lease executed work: exit=%s chapters=%d error=%v", code, len(after), err)
				}
			case "success and heartbeat", "peer connectivity", "host gateway connectivity":
				minimumRenewals := int32(2) // Import and runner validation.
				if scenario == "success and heartbeat" {
					minimumRenewals = 3 // Also require a background renewal.
				}
				if code != "0" || resultErr != nil || info.Status != jobdb.JobStatusCompleted || !strings.Contains(string(data), "container-result") || f.stats(t).Renewals < minimumRenewals {
					t.Fatalf("job did not complete through the supplied lease: exit=%s status=%s data=%s\n%s", code, info.Status, data, logs)
				}
				chapters, err := f.backend.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
				if err != nil {
					t.Fatal(err)
				}
				// The initial chapter only contains the submitted recipe; execution must
				// add persisted chapters/artifacts of its own.
				if len(chapters) < 2 {
					t.Fatal("executor did not persist execution chapters")
				}
				artifacts := 0
				for _, chapter := range chapters[1:] {
					for _, artifact := range chapter.Artifacts {
						reader, err := f.backend.OpenArtifact(ctx, jobdb.ArtifactRef{JobKey: key, Ordinal: chapter.Ordinal, Name: artifact.Name, Digest: artifact.Digest})
						if err != nil {
							t.Fatal(err)
						}
						stream, err := reader.Open()
						if err != nil {
							t.Fatal(err)
						}
						hash := sha256.New()
						size, readErr := io.Copy(hash, stream)
						closeErr := stream.Close()
						if readErr != nil || closeErr != nil || size != artifact.Size || fmt.Sprintf("%x", hash.Sum(nil)) != strings.TrimPrefix(artifact.Digest, "sha256:") {
							t.Fatalf("corrupt persisted artifact %s: size=%d read=%v close=%v", artifact.Name, size, readErr, closeErr)
						}
						artifacts++
					}
				}
				if artifacts == 0 {
					t.Fatal("executor did not persist execution artifacts")
				}
			case "recipe failure":
				if code == "0" || resultErr == nil || info.Status != jobdb.JobStatusCompleted || !hasStoredCommandExit(data, 23) {
					t.Fatalf("missing persisted command exit 23: exit=%s data=%s error=%v\n%s", code, data, resultErr, logs)
				}
			case "recipe timeout":
				// Scope-based execution reports the deadline as a structured timeout;
				// its diagnostic text need not repeat the configured recipe duration.
				var timeoutErr *jobdb.TimeoutError
				if code == "0" || info.Status != jobdb.JobStatusCompleted || !errors.As(resultErr, &timeoutErr) || timeoutErr.Payload.Scope != jobdb.TimeoutScopeTotal || timeoutErr.Payload.Code != "timeout_total" || timeoutErr.Payload.Retryable {
					t.Fatalf("missing total recipe timeout: exit=%s data=%s error=%v\n%s", code, data, resultErr, logs)
				}
			case "lease import transport failure":
				if code == "0" || info.Status == jobdb.JobStatusCompleted || f.stats(t).Renewals != 1 || !strings.Contains(logs, "cannot validate supplied lease") || !strings.Contains(logs, "request failed (phase exchange)") || !strings.Contains(logs, "job execution has not started") {
					t.Fatalf("missing lease import transport failure: exit=%s status=%s\n%s", code, info.Status, logs)
				}
				chapters, err := f.backend.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
				if err != nil || len(chapters) != 1 {
					t.Fatalf("failed lease import must not execute recipe steps: chapters=%d error=%v", len(chapters), err)
				}
			case "heartbeat transport failure":
				if code == "0" || info.Status == jobdb.JobStatusCompleted || f.stats(t).Renewals < 3 || !strings.Contains(logs, "lease renewal") {
					t.Fatalf("lost heartbeat did not stop execution: exit=%s status=%s\n%s", code, info.Status, logs)
				}
			}
			var inspected []struct {
				Config struct {
					Image                string
					Entrypoint, Cmd, Env []string
				}
				HostConfig struct {
					Binds      []string
					AutoRemove bool
				}
			}
			if err := json.Unmarshal([]byte("["+string(observation.inspection)+"]"), &inspected); err != nil || len(inspected) != 1 {
				t.Fatalf("inspect: %v", err)
			}
			c := inspected[0]
			if !c.HostConfig.AutoRemove || c.Config.Image != imageID || strings.Join(c.Config.Entrypoint, " ") != "c2j" || !strings.Contains(strings.Join(c.Config.Cmd, " "), "run with-lease") || len(c.HostConfig.Binds) != 0 || !strings.Contains(strings.Join(c.Config.Env, "\n"), "C2J_EXECUTION_IMAGE="+c2jIntegrationImage) || !strings.Contains(strings.Join(c.Config.Env, "\n"), "PULSE_EXECUTION_BASE="+baseName) || !strings.Contains(strings.Join(c.Config.Env, "\n"), "C2J_EXECUTION_PLATFORM="+platform) {
				t.Fatal("executor did not use the unmodified default image and direct c2j entrypoint")
			}
			if strings.Contains(strings.Join(c.Config.Env, "\n")+strings.Join(c.Config.Cmd, " "), "leaseToken") {
				t.Fatal("lease capability leaked into container metadata")
			}
		}) {
			// A failed launch can leave the shared Docker VM unhealthy. Do not
			// seed seven more namespaces and bury the first useful failure.
			t.Fatal("stopping Docker scenarios after first failure")
		}
	}
}

// The base image deliberately supplies Python rather than the mega image's curl.
func pythonHTTPGet(address string) string {
	return "python3 -c 'import sys, urllib.request; sys.stdout.buffer.write(urllib.request.build_opener(urllib.request.ProxyHandler({})).open(sys.argv[1], timeout=300).read())' " + fmt.Sprintf("%q", address)
}
