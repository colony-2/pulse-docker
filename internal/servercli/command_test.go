package servercli

import (
	"bytes"
	"os"
	"testing"
)

func TestCommandsHelpAndRejectedFlags(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	for _, name := range []string{"server", "setup", "upgrade", "status", "images", "launches", "logs"} {
		cmd := NewCommand("test-version")
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&out)
		cmd.SetArgs([]string{name, "--help"})
		if err := cmd.Execute(); err != nil || out.Len() == 0 {
			t.Fatal(name, err)
		}
		for _, flag := range []string{"--backend=vz", "--vm-home=/tmp", "--reconnect-attempts=2", "--shared-key=secret"} {
			cmd = NewCommand("test")
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs([]string{name, flag})
			if err := cmd.Execute(); err == nil {
				t.Fatal("accepted foreign flag", name, flag)
			}
		}
	}
	cmd := NewCommand("test-version")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"--version"})
	if err := cmd.Execute(); err != nil || !bytes.Contains(out.Bytes(), []byte("test-version")) {
		t.Fatal(err, out.String())
	}
	entries, _ := os.ReadDir(home)
	if len(entries) != 0 {
		t.Fatal("help mutated home")
	}
}
