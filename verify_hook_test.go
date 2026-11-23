package docker

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/colony-2/c2j/pkg/ops"
	"github.com/colony-2/c2j/pkg/recipe"
	"github.com/colony-2/c2j/pkg/worker/commandop"
	"gopkg.in/yaml.v3"
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
	t.Run("Docker execution requirements", func(t *testing.T) {
		needs := metadata.ExecutionNeeds
		if needs == nil || needs.Base != "ubuntu*" || len(needs.Capabilities) != 1 || needs.Capabilities[0] != "docker" || !reflect.ValueOf(needs.Resources).IsZero() {
			t.Fatalf("verify hook must request only Ubuntu and Docker: %+v", needs)
		}
	})
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

// Check the source mapping as well as parsed metadata: permissive decoding can
// discard unsupported fields, and zero-valued resources still declare a request.
func verifyHookRequirements(source []byte) error {
	var root map[string]any
	if err := yaml.Unmarshal(source, &root); err != nil {
		return err
	}
	if _, ok := root["execution_needs"]; !ok {
		return fmt.Errorf("missing execution_needs")
	}
	var walk func(any) error
	walk = func(value any) error {
		switch value := value.(type) {
		case map[string]any:
			for key, child := range value {
				if key == "execution_needs" {
					needs, ok := child.(map[string]any)
					if !ok || len(needs) != 2 || needs["base"] != "ubuntu*" || !reflect.DeepEqual(needs["capabilities"], []any{"docker"}) {
						return fmt.Errorf("execution_needs must contain only Ubuntu base and Docker capability: %v", child)
					}
				}
				if err := walk(child); err != nil {
					return err
				}
			}
		case []any:
			for _, child := range value {
				if err := walk(child); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return walk(root)
}

func TestVerifyHookRejectsUnsupportedRequirements(t *testing.T) {
	source, err := os.ReadFile(".c2/recipes/hooks/build/verify.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyHookRequirements(source); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"resources: {}", "resources: {memory: 8Gi}", "resources: {cpu: 4}", "cpu: 4", "core: 4", "cores: 4", "memory: 8Gi", "unexpected: true"} {
		t.Run(field, func(t *testing.T) {
			top := strings.Replace(string(source), "  capabilities: [docker]", "  capabilities: [docker]\n  "+field, 1)
			nested := strings.Replace(string(source), "- id: deterministic", "- id: deterministic\n  execution_needs:\n    base: ubuntu*\n    capabilities: [docker]\n    "+field, 1)
			for _, candidate := range []string{top, nested} {
				if err := verifyHookRequirements([]byte(candidate)); err == nil {
					t.Fatalf("unsupported requirement accepted: %s", candidate)
				}
			}
		})
	}
}

func TestVerifyHookRequiredCommandBounds(t *testing.T) {
	source, err := os.ReadFile(".c2/recipes/hooks/build/verify.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var hook struct {
		Sequence []struct {
			ID     string         `yaml:"id"`
			Op     string         `yaml:"op"`
			Inputs map[string]any `yaml:"inputs"`
		} `yaml:"sequence"`
	}
	if err := yaml.Unmarshal(source, &hook); err != nil {
		t.Fatal(err)
	}
	want := []struct{ id, run, timeout string }{
		{"deterministic", "./scripts/verify.sh", "25m"},
		{"live-cli", "./scripts/verify-live.sh cli", "65m"},
		{"live-remote", "./scripts/verify-live.sh remote", "65m"},
		{"live-linux", "./scripts/verify-live.sh linux", "65m"},
	}
	if len(hook.Sequence) != len(want) {
		t.Fatal("required command sequence changed")
	}
	for i, command := range hook.Sequence {
		in := command.Inputs
		if command.ID != want[i].id || command.Op != "command_execution" || in["run"] != want[i].run || in["timeout"] != want[i].timeout || in["capture_diagnostics"] != "always" {
			t.Fatalf("required command or bound changed: %+v", command)
		}
		if continuation, present := in["continue_on_error"]; present && continuation != false {
			t.Fatalf("required command can hide failures or timeouts: %+v", command)
		}
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
 test)
  if [ "$HARNESS_FAIL" = fixture-gate ] && echo "$*" | grep -q './testdata/jobdb-fixture'; then echo 'controlled required failure' >&2; exit 27; fi
  case "$*" in
   *-list*)
    case "$*" in
     *-tags=integration*)
      if [ "$HARNESS_FAIL" = integration-selection ]; then echo 'controlled required failure' >&2; exit 26; fi
      if [ "$HARNESS_FAIL" = cli-selection ]; then echo 'controlled required failure' >&2; else echo TestDockerCLIIntegration; fi
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
	}{{"test", 23}, {"selection", 24}, {"graph", 25}, {"integration-selection", 26}, {"completion-selection", 1}, {"removal-selection", 1}, {"moved-selection", 1}, {"since-selection", 1}, {"transport-selection", 1}, {"partial-connection-selection", 1}, {"inbound-validation-selection", 1}, {"cli-selection", 1}, {"fixture-gate", 27}} {
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
