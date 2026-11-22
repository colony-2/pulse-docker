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
	for _, p := range parents {
		data += event("run", p) + event("pass", p)
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
}
