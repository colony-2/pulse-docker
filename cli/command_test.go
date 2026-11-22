package cli_test

import (
	"bytes"
	"context"
	"github.com/colony-2/pulse-docker/cli"
	"github.com/spf13/cobra"
	"testing"
)

func TestEmbeddedCommandsInheritContextStreamsAndHooks(t *testing.T) {
	for range 2 {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		var out bytes.Buffer
		hook := false
		parent := &cobra.Command{Use: "host", PersistentPreRun: func(cmd *cobra.Command, _ []string) {
			hook = true
			if cmd.Context() != ctx {
				t.Error("lost context")
			}
		}}
		parent.PersistentFlags().String("host-option", "value", "host setting")
		parent.SetOut(&out)
		parent.SetErr(&out)
		parent.AddCommand(cli.NewStatusCommand())
		parent.SetArgs([]string{"status", "--socket", "unix:///nonexistent/socket"})
		if err := parent.ExecuteContext(ctx); err == nil || !hook {
			t.Fatal("context or hook lost", err, hook)
		}
	}
	a, b := cli.NewCommand("a"), cli.NewCommand("b")
	if a == b || a.Commands()[0] == b.Commands()[0] {
		t.Fatal("commands reused")
	}
}
