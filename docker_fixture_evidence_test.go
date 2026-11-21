//go:build integration

package docker

import (
	"encoding/json"
	"strings"
	"testing"
)

// Check the persisted outcome rather than c2j's changeable display text.
func hasStoredCommandExit(data []byte, expected int) bool {
	var stored struct {
		Attrs struct {
			Failure struct {
				Kind  string
				Node  struct{ Op string }
				Attrs struct {
					Command *struct {
						ExitCode *int `json:"exit_code"`
						TimedOut bool `json:"timed_out"`
					}
				}
			} `json:"c2j_failure"`
		}
	}
	if err := json.Unmarshal(data, &stored); err != nil {
		return false
	}
	failure := stored.Attrs.Failure
	command := failure.Attrs.Command
	return failure.Kind == "task_error" && failure.Node.Op == "command_execution" && command != nil && command.ExitCode != nil && *command.ExitCode == expected && !command.TimedOut
}

func TestDockerLeaseFailureEvidence(t *testing.T) {
	// The relevant stored fields from the live run whose display changed to
	// "intentional-recipe-failure [exit 23]". Artifact references are immaterial.
	const recorded = `{"message":"intentional-recipe-failure [exit 23]","attrs":{"c2j_failure":{"kind":"task_error","node":{"op":"command_execution"},"attrs":{"command":{"version":2,"exit_code":23,"timed_out":false}}}}}`
	for _, tc := range []struct {
		name, data string
		want       bool
	}{
		{"recorded outcome", recorded, true},
		{"different display", strings.ReplaceAll(recorded, "intentional-recipe-failure [exit 23]", "different wording"), true},
		{"display cannot override exit", strings.ReplaceAll(recorded, `"exit_code":23`, `"exit_code":7`), false},
		{"timeout is not command failure", strings.ReplaceAll(recorded, `"timed_out":false`, `"timed_out":true`), false},
		{"display without evidence", `{"message":"command execution failed: exit status 23"}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := hasStoredCommandExit([]byte(tc.data), 23); got != tc.want {
				t.Fatalf("persisted exit evidence: got %v want %v", got, tc.want)
			}
		})
	}
}
