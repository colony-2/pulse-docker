package docker

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestVerifyConsumerReference(t *testing.T) {
	const ref = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const checksum = "h1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
	for _, tc := range []struct {
		name, failure string
		origin        bool
		hash, version string
		exit          int
	}{
		{name: "origin identity", origin: true, hash: ref},
		{name: "Git fallback", hash: ref},
		{name: "wrong origin", origin: true, hash: strings.Repeat("b", 40), exit: 2},
		{name: "wrong fallback", hash: strings.Repeat("b", 40), exit: 2},
		{name: "wrong origin version", origin: true, hash: ref, version: "v0.0.0-20261121051419-bbbbbbbbbbbb", exit: 2},
		{name: "wrong version mapping", hash: ref, version: "v0.0.0-20261121051419-bbbbbbbbbbbb", exit: 2},
		{name: "invalid version", hash: ref, version: "invalid", exit: 2},
		{name: "unresolvable", failure: "get", exit: 23},
		{name: "download failure", failure: "download", exit: 26},
		{name: "graph failure", failure: "graph", exit: 24},
		{name: "compiler failure", failure: "compile", exit: 25},
		{name: "replacement", failure: "replacement", exit: 1},
		{name: "transitive Pulse", failure: "pulse", exit: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			hash := tc.hash
			if hash == "" {
				hash = ref
			}
			version := tc.version
			if version == "" {
				version = "v0.0.0-20261121051419-aaaaaaaaaaaa"
			}
			metadata := map[string]any{"Path": "github.com/colony-2/pulse-docker", "Version": version, "Sum": checksum}
			if tc.origin {
				metadata["Origin"] = map[string]string{"VCS": "git", "URL": "https://github.com/colony-2/pulse-docker", "Hash": tc.hash}
			}
			data, _ := json.Marshal(metadata)
			if err := os.WriteFile(filepath.Join(dir, "module"), data, 0600); err != nil {
				t.Fatal(err)
			}
			verificationStub(t, dir, "go", `if [ "$1" = build ]; then exec "$REAL_GO" "$@"; fi
case "$*" in
 'mod init '*) pwd > "$FAKE_SCRATCH";;
 get*) if [ "$FAKE_FAILURE" = get ]; then exit 23; fi;;
 'mod download '*) if [ "$FAKE_FAILURE" = download ]; then exit 26; fi; cat "$FAKE_DIR/module";;
 *Replace*) if [ "$FAKE_FAILURE" = replacement ]; then echo example.test/replaced; fi;;
 'list -mod=readonly -m all')
  if [ "$FAKE_FAILURE" = graph ]; then exit 24; fi
  echo github.com/colony-2/pulse-docker
  echo 'github.com/colony-2/pulse-provider-api v0.0.0-20261105011626-401f11e595a8'
  if [ "$FAKE_FAILURE" = pulse ]; then echo 'github.com/colony-2/pulse v0.0.1'; fi;;
 test*) if [ "$FAKE_FAILURE" = compile ]; then exit 25; fi;;
esac
`)
			verificationStub(t, dir, "git", `case "$*" in
 rev-parse*) echo "$FAKE_GIT_HASH";;
 show*) echo 2026-11-21T05:14:19Z;;
esac
`)
			cmd := exec.CommandContext(t.Context(), "bash", "scripts/verify-consumer.sh", ref)
			scratchFile := filepath.Join(dir, "scratch")
			cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "REAL_GO="+verificationGo(t), "FAKE_DIR="+dir, "FAKE_SCRATCH="+scratchFile, "FAKE_FAILURE="+tc.failure, "FAKE_GIT_HASH="+hash, "GOWORK=/invalid-workspace", "GOFLAGS=-mod=vendor")
			out, err := cmd.CombinedOutput()
			code := 0
			if err != nil {
				if e, ok := err.(*exec.ExitError); ok {
					code = e.ExitCode()
				} else {
					t.Fatal(err)
				}
			}
			if code != tc.exit || (code == 0 && (!strings.Contains(string(out), "requested_git_hash="+ref) || !strings.Contains(string(out), "module_checksum="+checksum) || !strings.Contains(string(out), "Resulting reference consumer passed"))) || (code != 0 && strings.Contains(string(out), "Resulting reference consumer passed")) {
				t.Fatalf("exit=%d want=%d: %s", code, tc.exit, out)
			}
			scratch, err := os.ReadFile(scratchFile)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(strings.TrimSpace(string(scratch))); !os.IsNotExist(err) {
				t.Fatal("scratch module survived", err)
			}
		})
	}
}
