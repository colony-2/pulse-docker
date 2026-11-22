package docker

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/colony-2/c2j/pkg/ops"
	"github.com/colony-2/c2j/pkg/recipe"
	"github.com/colony-2/c2j/pkg/worker/commandop"
)

func TestVerifyHookInputContract(t *testing.T) {
	previous := ops.List()
	ops.Register(commandop.GetOp())
	t.Cleanup(func() { ops.Replace(previous...) })
	b, err := os.ReadFile(".c2/recipes/hooks/build/verify.yaml")
	if err != nil {
		t.Fatal(err)
	}
	hook, err := recipe.LoadRecipeFromString(b)
	if err != nil {
		t.Fatal(err)
	}
	inputs := func() map[string]any {
		return map[string]any{
			"api_version":      "c2.build-hook/v1",
			"request":          "verify $(touch never-execute)",
			"target_directory": "; touch never-execute",
			"candidate":        "candidate; $(touch never-execute)",
			"base":             "base",
			"documents":        map[string]any{},
		}
	}
	metadata := hook.GetMetadata()
	t.Run("build runner metadata", func(t *testing.T) {
		got, err := metadata.ValidateInputShapeAndFillDefaults(inputs())
		if err != nil {
			t.Fatal(err)
		}
		for key, want := range inputs() {
			if key != "documents" && got[key] != want {
				t.Fatalf("%s changed: %v", key, got[key])
			}
		}
	})
	t.Run("artifact bindings", func(t *testing.T) {
		values := inputs()
		values["documents"] = map[string]any{"review/owner.md": map[string]any{"kind": "attached", "name": "owner.md"}}
		values["settings"] = map[string]any{"kind": "attached", "name": "settings.json"}
		if _, err := metadata.ValidateInputShapeAndFillDefaults(values); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("standalone defaults", func(t *testing.T) {
		got, err := metadata.ValidateInputShapeAndFillDefaults(nil)
		if err != nil || got["candidate"] != "working-tree" || got["api_version"] != "c2.build-hook/v1" || got["target_directory"] != "." || got["documents"] == nil {
			t.Fatalf("invalid standalone defaults: %v %v", got, err)
		}
	})
	for key, invalid := range map[string]any{
		"api_version": true, "request": 1, "target_directory": []any{"."},
		"candidate": false, "base": 2, "documents": "not an artifact map", "settings": 3,
		"unexpected": "unknown input",
	} {
		t.Run("reject "+key, func(t *testing.T) {
			values := inputs()
			values[key] = invalid
			if _, err := metadata.ValidateInputShapeAndFillDefaults(values); err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("invalid %s accepted or incorrectly diagnosed: %v", key, err)
			}
		})
	}
}

func TestVerifyHookPropagatesRequiredCommandFailure(t *testing.T) {
	b, err := os.ReadFile(".c2/recipes/hooks/build/verify.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "run: ./scripts/verify.sh") {
		t.Fatal("hook no longer invokes exercised entry point")
	}
	dir := t.TempDir()
	stub := `#!/bin/sh
case "$1" in
 version) echo 'go version controlled harness'; exit 0;;
 test) case "$*" in
   *-list*)
    case "$*" in
     *-tags=integration*)
      if [ "$HARNESS_FAIL" = integration-selection ]; then echo 'controlled required failure' >&2; exit 26; fi
      echo TestDockerLeaseFailureEvidence
      if [ "$HARNESS_FAIL" = removal-selection ]; then echo 'controlled required failure' >&2; else echo TestDockerRemoteFixtureRemovalObservation; fi
      if [ "$HARNESS_FAIL" = completion-selection ]; then echo 'controlled required failure' >&2; else echo TestDockerCompletedLeaseFixture; fi;;
     *)
      required=$(printf '%s' "$*" | sed -n 's/.*-list \^\([^$]*\)\$.*/\1/p')
      if [ "$HARNESS_FAIL" = inbound-validation-selection ] && [ "$required" = TestInboundSDKRejectsInvalidBaseDirBeforeConstruction ]; then echo 'controlled required failure' >&2; exit 0; fi
      if [ "$HARNESS_FAIL" = since-selection ] && [ "$required" = TestLaunchesSinceDurationAndTimestamp ]; then echo 'controlled required failure' >&2; exit 0; fi
      if [ "$HARNESS_FAIL" = transport-selection ] && [ "$required" = TestManagementValidatesTransportBeforeEffects ]; then echo 'controlled required failure' >&2; exit 0; fi
      if [ "$HARNESS_FAIL" = partial-connection-selection ] && [ "$required" = TestManagementAllowsIncompleteConnectionSettings ]; then echo 'controlled required failure' >&2; exit 0; fi
      if [ "$HARNESS_FAIL" = moved-selection ] && [ "$required" = TestLogsFastCaptureBeforeStartAndAfterRemoval ]; then echo 'controlled required failure' >&2; else echo "$required"; fi;;
    esac
    if [ "$HARNESS_FAIL" = selection ]; then echo 'controlled required failure' >&2; exit 24; fi; exit 0;;
   *) if [ "$HARNESS_FAIL" = test ]; then echo 'controlled required failure' >&2; exit 23; fi; exit 0;;
  esac;;
 list) echo github.com/colony-2/pulse-docker; echo 'controlled required failure' >&2; exit 25;;
 *) exit 0;;
esac
`
	if err := os.WriteFile(filepath.Join(dir, "go"), []byte(stub), 0700); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		code int
	}{{"test", 23}, {"selection", 24}, {"graph", 25}, {"integration-selection", 26}, {"completion-selection", 1}, {"removal-selection", 1}, {"moved-selection", 1}, {"since-selection", 1}, {"transport-selection", 1}, {"partial-connection-selection", 1}, {"inbound-validation-selection", 1}} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), "bash", "./scripts/verify.sh")
			cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "HARNESS_FAIL="+tc.name)
			out, err := cmd.CombinedOutput()
			exit, ok := err.(*exec.ExitError)
			if !ok || exit.ExitCode() != tc.code || !strings.Contains(string(out), "controlled required failure") || strings.Contains(string(out), "verification passed") {
				t.Fatalf("hook hid required failure: %v\n%s", err, out)
			}
		})
	}
}
