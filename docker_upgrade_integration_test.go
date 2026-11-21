//go:build integration

package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	compute "github.com/colony-2/pulse-docker/internal/native"
	provider "github.com/colony-2/pulse-provider-api"
)

// Inject recorded original metadata on a real fixture worker/cache pair. The
// holding process is instrumentation; supplied-lease execution is tested separately.
func TestDockerUpgradeIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	socket, err := ResolveSocket("")
	if err != nil {
		t.Fatal(err)
	}
	command := func(args ...string) (string, error) {
		out, err := exec.CommandContext(ctx, "docker", append([]string{"--host", socket}, args...)...).CombinedOutput()
		return strings.TrimSpace(string(out)), err
	}
	docker := func(args ...string) string {
		t.Helper()
		out, err := command(args...)
		if err != nil {
			t.Fatalf("docker %v: %v\n%s", args, err, out)
		}
		return out
	}
	platform := docker("info", "--format", "{{.OSType}}/{{.Architecture}}")
	platform = strings.ReplaceAll(strings.ReplaceAll(platform, "aarch64", "arm64"), "x86_64", "amd64")
	fixtures := originalFixtures(t)
	f := fixtures[0]
	if platform == "linux/amd64" {
		f = fixtures[1]
	}
	// Resolve the original wildcard to its recorded concrete base; both produce
	// exactly the same native JSON/fingerprint in the original adapter.
	if f.Launch.Base == "ubuntu-26*" {
		f.Launch.Base = "ubuntu-26.04-arm64"
	}
	var native compute.Launch
	if err := json.Unmarshal([]byte(f.NativeJSON), &native); err != nil {
		t.Fatal(err)
	}
	var cost charge
	if err := json.Unmarshal([]byte(f.Accounting), &cost); err != nil {
		t.Fatal(err)
	}
	if _, err := command("inspect", f.Name); err == nil {
		t.Fatal("original fixture name already exists; refusing to touch it")
	}
	tools := "pulse-cache-v2-" + f.CacheKey + "-tools"
	nix := "pulse-cache-v2-" + f.CacheKey + "-nix"
	for _, name := range []string{tools, nix} {
		if _, err := command("volume", "inspect", name); err == nil {
			t.Fatal("original fixture cache already exists; refusing to touch it", name)
		}
	}
	docker("pull", "--platform", native.Platform, c2jIntegrationImage)
	image := docker("image", "inspect", "--format", "{{.Id}}", c2jIntegrationImage)
	sentinel := "pulse-docker-unrelated-" + compute.NewID()
	owned := []string{}
	var worker string
	var p *Provider
	t.Cleanup(func() {
		if p != nil {
			p.Close()
		}
		cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		if worker != "" {
			out, err := exec.CommandContext(cleanup, "docker", "--host", socket, "rm", "-f", worker).CombinedOutput()
			if err != nil {
				t.Errorf("remove upgrade worker: %v: %s", err, out)
			}
		}
		for _, name := range owned {
			out, err := exec.CommandContext(cleanup, "docker", "--host", socket, "volume", "rm", name).CombinedOutput()
			if err != nil {
				t.Errorf("remove fixture volume: %v: %s", err, out)
			}
		}
	})
	for _, name := range []string{tools, nix} {
		args := []string{"volume", "create"}
		for k, v := range f.CacheLabels {
			args = append(args, "--label", k+"="+v)
		}
		args = append(args, name)
		owned = append(owned, name)
		docker(args...)
	}
	docker("volume", "create", sentinel)
	owned = append(owned, sentinel)
	labels := metadata(f.Launch)
	labels["pulse_process_fingerprint"] = f.Fingerprint
	labels[accountingLabel] = f.Accounting
	labels[limitsLabel] = "enforced"
	labels[roleLabel] = "worker"
	labels[cacheKeyLabel] = f.CacheKey
	labels[cacheLayoutLabel] = "2"
	args := []string{"run", "-d", "--rm", "--name", f.Name, "--platform", native.Platform, "--restart", "no", "--cpus", fmt.Sprintf("%.3f", float64(cost.CPU)/1000), "--mount", "type=volume,source=" + tools + ",target=/var/cache/pulse", "--mount", "type=volume,source=" + nix + ",target=/nix"}
	if native.MemoryBytes > 0 {
		args = append(args, "--memory", fmt.Sprint(native.MemoryBytes), "--memory-swap", fmt.Sprint(native.MemoryBytes))
	}
	for k, v := range labels {
		args = append(args, "--label", k+"="+v)
	}
	env := cacheEnvironment(image)
	env["NIX_CONFIG"] = "\nmin-free = 0\n"
	for k, v := range env {
		args = append(args, "--env", k+"="+v)
	}
	args = append(args, "--entrypoint", "/bin/sh", image, "-ec", "echo original-tools > /var/cache/pulse/upgrade-marker; echo original-nix > /nix/upgrade-marker; touch /nix/.pulse-ready; while [ ! -e /tmp/release ]; do sleep 0.1; done")
	worker = f.Name
	worker = docker(args...)
	docker("exec", worker, "/bin/sh", "-ec", "until [ -f /nix/.pulse-ready ]; do sleep 0.1; done")
	started := docker("inspect", "--format", "{{.State.StartedAt}}", worker)
	// Use a fresh maintenance owner while recognizing the original cache labels.
	// Startup retirement must never touch another test or host namespace.
	cfg := Config{Socket: socket, LockDir: t.TempDir(), MaxContainers: 1, ExecutorEnv: f.Defaults, DependencyCache: CacheConfig{Namespace: "upgrade-fixture-" + compute.NewID(), MaxAge: time.Nanosecond}}
	for cycle := 0; cycle < 2; cycle++ {
		p, err = New(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if other, err := New(ctx, cfg); err == nil {
			other.Close()
			t.Fatal("second admission owner succeeded")
		}
		out, err := p.Submit(ctx, []provider.Launch{f.Launch})
		if err != nil || len(out) != 1 || out[0].Status != provider.Accepted {
			t.Fatal("original worker not recognized", out, err)
		}
		blocked := assignment("upgrade-blocked-" + compute.NewID())
		out, err = p.Submit(ctx, []provider.Launch{blocked})
		if err != nil || len(out) != 1 || out[0].Status != provider.NoCapacity {
			t.Fatal("accounting was not restored", out, err)
		}
		page, err := p.List(ctx, provider.ListRequest{LaunchID: f.Launch.LaunchID})
		if err != nil || len(page.Items) != 1 || page.Items[0].ID != worker || page.Items[0].Job != f.Launch.Job {
			t.Fatal(page, err)
		}
		if err := p.Maintain(ctx); err != nil {
			t.Fatal(err)
		}
		p.Close()
		if got := docker("inspect", "--format", "{{.State.StartedAt}}", worker); got != started {
			t.Fatal("worker restarted")
		}
		if got := docker("exec", worker, "/bin/sh", "-ec", "cat /var/cache/pulse/upgrade-marker /nix/upgrade-marker"); got != "original-tools\noriginal-nix" {
			t.Fatal("cache reset", got)
		}
		docker("volume", "inspect", sentinel)
	}
	if ids := strings.Fields(docker("ps", "-aq", "--no-trunc", "--filter", "label=pulse_launch_id="+f.Launch.LaunchID)); len(ids) != 1 || ids[0] != worker {
		t.Fatal("duplicate original worker", ids)
	}
}
