package servercli

import (
	"bytes"
	"context"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func executeOptions(t *testing.T, name string, args ...string) (serverOptions, error) {
	t.Helper()
	var got serverOptions
	cmd := command(name, "test", func(_ context.Context, o serverOptions, _ []string) error { got = o; return nil })
	cmd.SetOut(new(bytes.Buffer))
	cmd.SetErr(new(bytes.Buffer))
	cmd.SetArgs(args)
	return gotAfter(cmd, &got)
}
func gotAfter(cmd *cobra.Command, got *serverOptions) (serverOptions, error) {
	err := cmd.Execute()
	return *got, err
}
func TestConfigurationPrecedencePathsAndPrivateSave(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	dir := t.TempDir()
	path := filepath.Join(dir, "provider.json")
	data := `{"version":1,"flags":{"cpu-millis":"500","log-directory":"logs","executor-env-file":"env.json","poll-interval":"1s"}}`
	os.WriteFile(path, []byte(data), 0600)
	os.WriteFile(filepath.Join(dir, "env.json"), []byte(`{"CUSTOM":"private-value"}`), 0600)
	o, err := executeOptions(t, "status", "--config", path, "--cpu-millis", "900")
	if err != nil || o.config.CPUMillis != 900 || o.logging.Directory != filepath.Join(dir, "logs") || o.config.ExecutorEnv["CUSTOM"] != "private-value" || o.poll != time.Second {
		t.Fatal(o, err)
	}
	cmd := command("setup", "test", func(context.Context, serverOptions, []string) error { return nil })
	cmd.SetArgs([]string{"--executor-env-file", filepath.Join(dir, "env.json")})
	if err = cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	cmd.Parent()
	saved := filepath.Join(dir, "saved.json")
	if err = saveConfiguration(cmd, saved); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(saved)
	st, _ := os.Stat(saved)
	if st.Mode().Perm() != 0600 || bytes.Contains(b, []byte("private-value")) {
		t.Fatal(st, string(b))
	}
	var cfg savedConfiguration
	if strictJSON(b, &cfg) != nil || cfg.Flags["executor-env-file"] != filepath.Join(dir, "env.json") {
		t.Fatal(string(b))
	}
}
func TestConfigurationRejectsMalformedBeforeDaemon(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	path := filepath.Join(t.TempDir(), "config")
	for _, data := range []string{`{"version":1,"version":1,"flags":{}}`, `{"version":1,"flags":{"cpu-millis":"1","cpu-millis":"2"}}`, `{"version":2,"flags":{}}`, `{"version":1,"flags":{"unknown":"x"}}`, `{"version":1,"flags":{"memory-bytes":4}}`, `{"version":1,"flags":{},"secret":"x"}`, `{} {}`, strings.Repeat(" ", 1<<20) + `{}`} {
		os.WriteFile(path, []byte(data), 0600)
		if _, err := executeOptions(t, "status", "--config", path); err == nil {
			t.Fatal("accepted malformed config", data[:min(len(data), 120)])
		}
	}
	if _, err := executeOptions(t, "status", "--config", path+"missing"); err == nil {
		t.Fatal("explicit missing accepted")
	}
	if _, err := executeOptions(t, "setup", "--config", path+"missing"); err != nil {
		t.Fatal(err)
	}
	if _, err := executeOptions(t, "status"); err != nil {
		t.Fatal(err)
	}
}
func TestConfigurationRejectsStaticAndSecretErrors(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	for _, args := range [][]string{{"--cpu-millis", "-1"}, {"--memory-bytes", "-1"}, {"--max-containers", "-1"}, {"--overhead-bytes", "-1"}, {"--scratch-path", "/"}, {"--poll-interval", "0s"}, {"--owner", "../foreign"}, {"--log-max-bytes", "-1"}, {"--cache-max-age", "-1h"}, {"--socket", "tcp://localhost:2375"}, {"--limit", "-1"}, {"--since", "yesterday"}, {"--since", "-1h"}, {"--since", "2562048h"}} {
		if _, err := executeOptions(t, "launches", args...); err == nil {
			t.Fatal("accepted", args)
		}
	}
	dir := t.TempDir()
	for _, data := range []string{`{"C2J_JOBDB":"secret"}`, `{"PULSE_HIDDEN":"secret"}`, `{"C2J_EXECUTION_CPU":"secret"}`, `{"CUSTOM":4}`} {
		file := filepath.Join(dir, "env")
		os.WriteFile(file, []byte(data), 0600)
		if _, err := executeOptions(t, "status", "--executor-env-file", file); err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatal(err)
		}
	}
	o, err := executeOptions(t, "status")
	if err != nil || o.config.CPUMillis != 0 || o.config.MemoryBytes != 0 || o.config.MaxContainers != 0 {
		t.Fatal(o, err)
	}
	file := filepath.Join(dir, "auth")
	os.WriteFile(file, []byte("private-invalid"), 0600)
	if _, err = executeOptions(t, "status", "--registry-auth-file", file); err == nil || strings.Contains(err.Error(), "private-invalid") {
		t.Fatal(err)
	}
}
func TestConfigurationSaveFailurePreservesPrevious(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "config")
	os.Mkdir(target, 0700)
	os.WriteFile(filepath.Join(target, "sentinel"), []byte("previous"), 0600)
	cmd := NewSetupCommand()
	if err := saveConfiguration(cmd, target); err == nil {
		t.Fatal("rename unexpectedly succeeded")
	}
	b, err := os.ReadFile(filepath.Join(target, "sentinel"))
	if err != nil || string(b) != "previous" {
		t.Fatal(string(b), err)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatal("temporary config leaked", entries)
	}
}

func TestManagementValidatesCredentialsBeforeEffects(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("PULSE_TEST_INVALID_KEY", "private-value\nsecond-line")
	dir := t.TempDir()
	bad := filepath.Join(dir, "malformed")
	if err := os.WriteFile(bad, []byte("private-value\nsecond-line"), 0600); err != nil {
		t.Fatal(err)
	}
	_, trustFlags := fixtureTrust(t)
	for _, name := range []string{"setup", "upgrade"} {
		for _, flags := range [][]string{
			{"--tls-ca", bad},
			{"--tls-cert", bad, "--tls-key", trustFlags[5]},
			{"--tls-cert", trustFlags[3], "--tls-key", bad},
			{"--tls-cert", trustFlags[3]},
			{"--tls-key", trustFlags[5]},
			{"--shared-key-file", bad},
			{"--shared-key-file", bad + "-missing"},
			{"--shared-key-env", "PULSE_TEST_INVALID_KEY"},
			{"--shared-key-file", bad, "--shared-key-env", "PULSE_TEST_INVALID_KEY"},
		} {
			t.Run(name+strings.Join(flags, "/"), func(t *testing.T) {
				if _, err := executeOptions(t, name, flags...); err == nil || strings.Contains(err.Error(), "private-value") {
					t.Fatalf("malformed credentials reached management or leaked: %v", err)
				}
			})
		}
		if _, err := executeOptions(t, name, trustFlags...); err != nil {
			t.Fatalf("valid TLS files rejected: %v", err)
		}
		key := filepath.Join(dir, "valid-key")
		if err := os.WriteFile(key, []byte(" exact fixture key \n"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := executeOptions(t, name, "--reverse-auth", "mutual-key", "--shared-key-file", key); err != nil {
			t.Fatalf("valid key file rejected: %v", err)
		}
	}
}
