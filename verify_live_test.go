package docker

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func verificationStub(t *testing.T, dir, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n"+body), 0700); err != nil {
		t.Fatal(err)
	}
}
func verificationGo(t *testing.T) string {
	t.Helper()
	p, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestVerifyLiveResults(t *testing.T) {
	const pkg = "github.com/colony-2/pulse-docker/internal/provider"
	const completedLease = "TestDockerLeaseIntegration/completed_lease"
	parents := []string{"TestDockerLifecycleResourcesIntegration", "TestDockerLeaseIntegration", "TestDockerDependencyCacheIntegration", "TestDockerNixFetchCacheIntegration", "TestDockerMultiPlatformIntegration", "TestDockerUpgradeIntegration"}
	event := func(action, name, output string) string {
		b, _ := json.Marshal(map[string]string{"Action": action, "Package": pkg, "Test": name, "Output": output})
		return string(b) + "\n"
	}
	stream := func(branch, leaseResult string) string {
		s := event("start", "", "")
		for _, parent := range parents {
			s += event("run", parent, "")
			if parent == parents[1] && leaseResult != "" {
				s += event("run", completedLease, "") + event(leaseResult, completedLease, "")
			}
			if parent == parents[4] && branch != "" {
				name := parent + "/" + branch
				s += event("run", name, "") + event("output", name, "D28 native=linux/arm64 alternative=linux/amd64\n") + event("pass", name, "")
			}
			s += event("pass", parent, "")
		}
		return s + event("pass", "", "")
	}
	safe := stream("unavailable_emulation_declines", "pass")
	verified := stream("verified_emulation", "pass")
	lease := event("start", "", "") + event("run", parents[1], "") + event("run", completedLease, "") + event("pass", completedLease, "") + event("pass", parents[1], "") + event("pass", "", "")
	cliParent := "TestDockerCLIIntegration"
	cliEvents := event("start", "", "") + event("run", cliParent, "")
	for _, scenario := range []string{"outbound_restart", "inbound_parent_loss"} {
		cliEvents += event("run", cliParent+"/"+scenario, "") + event("pass", cliParent+"/"+scenario, "")
	}
	cliEvents += event("run", cliParent+"/invalid_leases", "")
	for _, invalid := range []string{"expired", "completed"} {
		cliEvents += event("run", cliParent+"/invalid_leases/"+invalid, "") + event("pass", cliParent+"/invalid_leases/"+invalid, "")
	}
	cliEvents += event("pass", cliParent+"/invalid_leases", "")
	cliEvents += event("pass", cliParent, "") + event("pass", "", "")
	for _, tc := range []struct {
		name, mode, data, want     string
		exit, testExit, dockerExit int
		missingList                bool
	}{
		{name: "safe decline", mode: "linux", data: safe, want: "alternative execution unexercised"},
		{name: "verified emulation", mode: "linux", data: verified, want: "alternative=linux/amd64: verified execution passed"},
		{name: "Linux missing completed lease", mode: "linux", data: stream("unavailable_emulation_declines", ""), want: "required completed-lease subtest did not pass: " + completedLease, exit: 2},
		{name: "Linux skipped completed lease", mode: "linux", data: stream("unavailable_emulation_declines", "skip"), want: "required live coverage skip: " + completedLease, exit: 2},
		{name: "missing branch", mode: "linux", data: stream("", "pass"), exit: 2},
		{name: "skipped branch", mode: "linux", data: strings.Replace(safe, `"Action":"pass","Output":"","Package":"`+pkg+`","Test":"`+parents[4]+`/unavailable_emulation_declines"`, `"Action":"skip","Output":"","Package":"`+pkg+`","Test":"`+parents[4]+`/unavailable_emulation_declines"`, 1), exit: 2},
		{name: "conflicting branches", mode: "linux", data: strings.Replace(safe, event("pass", parents[4], ""), event("run", parents[4]+"/verified_emulation", "")+event("pass", parents[4]+"/verified_emulation", "")+event("pass", parents[4], ""), 1), exit: 2},
		{name: "unknown branch", mode: "linux", data: stream("other", "pass"), exit: 2},
		{name: "message alone", mode: "linux", data: strings.Replace(stream("", "pass"), event("pass", parents[4], ""), event("output", parents[4], "safe decline proved D28 native=linux/arm64 alternative=linux/amd64")+event("pass", parents[4], ""), 1), exit: 2},
		{name: "malformed", mode: "linux", data: safe + "{", exit: 2},
		{name: "truncated", mode: "linux", data: strings.TrimSuffix(safe, event("pass", "", "")), exit: 2},
		{name: "unrelated skip", mode: "linux", data: strings.Replace(safe, event("pass", parents[0], ""), event("skip", parents[0], ""), 1), exit: 2},
		{name: "missing suite", mode: "linux", data: strings.ReplaceAll(strings.ReplaceAll(safe, event("run", parents[0], ""), ""), event("pass", parents[0], ""), ""), exit: 2},
		{name: "empty selection", mode: "linux", data: safe, missingList: true, exit: 1},
		{name: "native failure", mode: "linux", data: safe, testExit: 17, exit: 17},
		{name: "daemon unavailable", mode: "linux", data: safe, dockerExit: 19, exit: 19},
		{name: "CLI recovery", mode: "cli", data: cliEvents},
		{name: "CLI parent only", mode: "cli", data: event("start", "", "") + event("run", cliParent, "") + event("pass", cliParent, "") + event("pass", "", ""), exit: 2},
		{name: "CLI missing invalid lease", mode: "cli", data: strings.ReplaceAll(strings.ReplaceAll(cliEvents, event("run", cliParent+"/invalid_leases/expired", ""), ""), event("pass", cliParent+"/invalid_leases/expired", ""), ""), exit: 2},
		{name: "CLI missing parent loss", mode: "cli", data: strings.ReplaceAll(strings.ReplaceAll(cliEvents, event("run", cliParent+"/inbound_parent_loss", ""), ""), event("pass", cliParent+"/inbound_parent_loss", ""), ""), exit: 2},
		{name: "CLI skipped", mode: "cli", data: strings.Replace(cliEvents, event("pass", cliParent+"/outbound_restart", ""), event("skip", cliParent+"/outbound_restart", ""), 1), exit: 2},
		{name: "CLI native failure", mode: "cli", data: cliEvents, testExit: 17, exit: 17},
		{name: "completed lease", mode: "lease-completion", data: lease, want: "lease-completion scope"},
		{name: "missing completed lease", mode: "lease-completion", data: event("start", "", "") + event("run", parents[1], "") + event("pass", parents[1], "") + event("pass", "", ""), exit: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			names := strings.Join(parents, "\n") + "\n"
			if tc.mode == "lease-completion" {
				names = parents[1] + "\n"
			}
			if tc.mode == "cli" {
				names = cliParent + "\n"
			}
			if tc.missingList {
				names = ""
			}
			for name, data := range map[string]string{"events": tc.data, "names": names} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0600); err != nil {
					t.Fatal(err)
				}
			}
			verificationStub(t, dir, "docker", "exit \"$FAKE_DOCKER_EXIT\"\n")
			verificationStub(t, dir, "go", `echo "$*" >> "$FAKE_CALLS"
if [ "$1" = build ]; then exec "$REAL_GO" "$@"; fi
case "$*" in
 *-list*) cat "$FAKE_DIR/names";;
 *-json*) cat "$FAKE_DIR/events"; exit "$FAKE_TEST_EXIT";;
esac
`)
			cmd := exec.CommandContext(t.Context(), "bash", "scripts/verify-live.sh", tc.mode)
			cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "REAL_GO="+verificationGo(t), "VERIFY_CANDIDATE=controlled", "FAKE_DIR="+dir, "FAKE_CALLS="+filepath.Join(dir, "calls"), fmtEnvInt("FAKE_TEST_EXIT", tc.testExit), fmtEnvInt("FAKE_DOCKER_EXIT", tc.dockerExit))
			out, err := cmd.CombinedOutput()
			code := 0
			if err != nil {
				if e, ok := err.(*exec.ExitError); ok {
					code = e.ExitCode()
				} else {
					t.Fatal(err)
				}
			}
			if code != tc.exit || (tc.want != "" && !strings.Contains(string(out), tc.want)) || (tc.exit != 0 && strings.Contains(string(out), "Live verification passed")) {
				t.Fatalf("exit=%d want=%d: %s", code, tc.exit, out)
			}
			calls, _ := os.ReadFile(filepath.Join(dir, "calls"))
			if strings.Contains(string(calls), "Desktop") {
				t.Fatal("Linux scheduled Desktop")
			}
			if tc.mode == "lease-completion" && !strings.Contains(string(calls), "^TestDockerLeaseIntegration$/^completed_lease$") {
				t.Fatal("missing explicit completed-lease selection")
			}
		})
	}
}

func fmtEnvInt(name string, n int) string { return name + "=" + strconv.Itoa(n) }
