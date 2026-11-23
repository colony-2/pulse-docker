package docker

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestVerifyRemoteRequiredResults(t *testing.T) {
	dir := t.TempDir()
	checker := filepath.Join(dir, "check")
	cmd := exec.CommandContext(t.Context(), "go", "build", "-o", checker, "scripts/verifycheck.go")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatal(err, string(b))
	}
	parents := []string{"TestDockerRemoteInboundLeaseIntegration", "TestDockerRemoteOutboundLeaseIntegration", "TestDockerFastCaptureIntegration", "TestDockerCaptureRestartIntegration", "TestDockerManagementIntegration"}
	event := func(action, name string) string {
		b, _ := json.Marshal(map[string]string{"Action": action, "Package": "github.com/colony-2/pulse-docker/internal/provider", "Test": name})
		return string(b) + "\n"
	}
	data := event("start", "")
	children := map[string][]string{parents[2]: {"exit_zero", "exit_nonzero", "oom"}, parents[3]: {"running_reopen", "stopped_reopen", "forced_removal", "rotation", "grace_expiry"}, parents[4]: {"read_launch_logs_beside_active_owner"}}
	for _, p := range parents {
		data += event("run", p)
		for _, child := range children[p] {
			data += event("run", p+"/"+child) + event("pass", p+"/"+child)
		}
		data += event("pass", p)
	}
	data += event("pass", "")
	for _, tc := range []struct {
		name, data string
		valid      bool
	}{{"complete", data, true}, {"missing", strings.ReplaceAll(strings.ReplaceAll(data, event("run", parents[0]), ""), event("pass", parents[0]), ""), false}, {"skipped", strings.Replace(data, event("pass", parents[2]), event("skip", parents[2]), 1), false}, {"truncated", strings.TrimSuffix(data, event("pass", "")), false}, {"malformed", data + "{", false}, {"contradictory", strings.Replace(data, event("pass", parents[1]), event("pass", parents[1])+event("fail", parents[1]), 1), false}} {
		t.Run(tc.name, func(t *testing.T) {
			file := filepath.Join(dir, "events")
			os.WriteFile(file, []byte(tc.data), 0600)
			b, err := exec.CommandContext(t.Context(), checker, "live", "remote", file).CombinedOutput()
			if (err == nil) != tc.valid || !tc.valid && strings.Contains(string(b), "Live verification passed") {
				t.Fatal(err, string(b))
			}
		})
	}
	t.Run("missing_reads_beside_active_owner", func(t *testing.T) {
		fixture := parents[4] + "/read_launch_logs_beside_active_owner"
		file := filepath.Join(t.TempDir(), "events")
		if err := os.WriteFile(file, []byte(strings.ReplaceAll(data, event("run", fixture)+event("pass", fixture), "")), 0600); err != nil {
			t.Fatal(err)
		}
		output, err := exec.CommandContext(t.Context(), checker, "live", "remote", file).CombinedOutput()
		if err == nil || !strings.Contains(string(output), fixture) {
			t.Fatal(err, string(output))
		}
	})
	for _, tc := range []struct {
		name   string
		mutate func(string, string) string
	}{
		{"missing OOM", func(data, name string) string {
			return strings.ReplaceAll(strings.ReplaceAll(data, event("run", name), ""), event("pass", name), "")
		}},
		{"skipped OOM", func(data, name string) string {
			return strings.Replace(data, event("pass", name), event("skip", name), 1)
		}},
		{"failed OOM", func(data, name string) string {
			return strings.Replace(data, event("pass", name), event("fail", name), 1)
		}},
		{"incomplete OOM", func(data, name string) string { return strings.Replace(data, event("pass", name), "", 1) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "events")
			if err := os.WriteFile(file, []byte(tc.mutate(data, parents[2]+"/oom")), 0600); err != nil {
				t.Fatal(err)
			}
			output, err := exec.CommandContext(t.Context(), checker, "live", "remote", file).CombinedOutput()
			if err == nil || strings.Contains(string(output), "Live verification passed") {
				t.Fatal(err, string(output))
			}
		})
	}
}
