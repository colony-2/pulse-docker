//go:build integration

package provider

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestDockerDesktopIntegration(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Fatal("required prerequisite: an actual macOS Docker Desktop host")
	}
	t.Setenv("DOCKER_HOST", "")
	t.Setenv("DOCKER_CONTEXT", "default")
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	// Explicit default suppresses saved contexts; discovery must find the user's
	// Desktop socket without relying on a /var/run/docker.sock symlink.
	t.Setenv("DOCKER_CONTEXT", "")
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	want, err := ResolveSocket("unix://" + filepath.Join(home, ".docker", "run", "docker.sock"))
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(strings.TrimPrefix(want, "unix://"))
	if err != nil || info.Mode()&os.ModeSocket == 0 {
		t.Fatalf("required prerequisite: Docker Desktop user's Unix socket %s: %v", want, err)
	}
	got, err := ResolveSocket("")
	if err != nil || got != want {
		t.Fatal("user socket discovery", got, want, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	identity, err := exec.CommandContext(ctx, "docker", "--host", want, "info", "--format", "{{.OperatingSystem}}").CombinedOutput()
	if err != nil || !strings.Contains(strings.ToLower(string(identity)), "docker desktop") {
		t.Fatalf("required prerequisite: actual Docker Desktop daemon; identity=%q error=%v", identity, err)
	}
	t.Logf("Desktop daemon: %s", strings.TrimSpace(string(identity)))
	TestDockerLeaseIntegration(t)
}
