package servercli

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/colony-2/pulse-provider-api/plugin"
)

func TestManagementValidatesTransportBeforeEffects(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	socket := filepath.Join(t.TempDir(), "docker.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	var requests atomic.Int64
	daemon := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	daemon.Listener.Close()
	daemon.Listener = listener
	daemon.Start()
	t.Cleanup(daemon.Close)

	for _, name := range []string{"setup", "upgrade"} {
		for _, tc := range []struct {
			flags []string
			want  string
		}{
			{[]string{"--rpc-concurrency", "-1"}, "--rpc-concurrency"},
			{[]string{"--rpc-concurrency", "0"}, "--rpc-concurrency"},
			{[]string{"--rpc-concurrency", "65537"}, "--rpc-concurrency"},
			{[]string{"--max-message-bytes", "1023"}, "--max-message-bytes"},
			{[]string{"--max-message-bytes", strconv.Itoa(plugin.MaxMessageBytes + 1)}, "--max-message-bytes"},
			{[]string{"--startup-timeout", "0s"}, "timeouts must be positive"},
			{[]string{"--shutdown-timeout", "-1s"}, "timeouts must be positive"},
			{[]string{"--reconnect-initial-delay", "0s"}, "reconnect delay bounds"},
			{[]string{"--reconnect-max-delay", "1ms"}, "reconnect delay bounds"},
			{[]string{"--reconnect-reset-after", "-1s"}, "reconnect delay bounds"},
			{[]string{"--reverse-auth", "unknown"}, "--reverse-auth"},
			{[]string{"--reverse-transport", "unknown"}, "--reverse-transport"},
			{[]string{"--reverse-transport", "http"}, "http requires mutual-key"},
			{[]string{"--collector", "invalid-address"}, "--collector=host:port"},
			{[]string{"--mode", "inbound", "--collector", "localhost:8443"}, "require outbound mode"},
			{[]string{"--shared-key-env", "KEY"}, "require --reverse-auth=mutual-key"},
			{[]string{"--reverse-auth", "mutual-key", "--shared-key-env", "KEY", "--shared-key-file", "missing"}, "exactly one"},
			{[]string{"--reverse-auth", "mutual-key", "--tls-cert", "missing"}, "require --reverse-auth=mtls"},
			{[]string{"--reverse-auth", "mutual-key", "--shared-key-env", "INVALID=NAME"}, "invalid --shared-key-env"},
			{[]string{"--reverse-auth", "mutual-key", "--reverse-transport", "http", "--tls-ca", "missing"}, "--tls-* options"},
			{[]string{"--reverse-auth", "mutual-key", "--reverse-transport", "http", "--tls-server-name", "host"}, "--tls-* options"},
			{[]string{"--tls-cert", "missing"}, "must be supplied together"},
			{[]string{"--tls-key", "missing"}, "must be supplied together"},
			{[]string{"--name", "invalid\nidentity"}, "valid provider identity"},
		} {
			for _, source := range []string{"flags", "saved"} {
				t.Run(name+"/"+source+"/"+strings.Join(tc.flags, " "), func(t *testing.T) {
					path := filepath.Join(t.TempDir(), "provider.json")
					settings := savedConfiguration{Version: 1, Flags: map[string]string{}}
					args := []string{name, "--socket", "unix://" + socket, "--config", path}
					if source == "saved" {
						for i := 0; i < len(tc.flags); i += 2 {
							settings.Flags[strings.TrimPrefix(tc.flags[i], "--")] = tc.flags[i+1]
						}
					} else {
						args = append(args, tc.flags...)
					}
					before, err := json.Marshal(settings)
					if err != nil {
						t.Fatal(err)
					}
					if err = os.WriteFile(path, before, 0600); err != nil {
						t.Fatal(err)
					}
					cmd := NewCommand("test")
					var output bytes.Buffer
					cmd.SetOut(&output)
					cmd.SetErr(&output)
					cmd.SetArgs(args)
					beforeRequests := requests.Load()
					if err = cmd.Execute(); err == nil || !strings.Contains(err.Error(), tc.want) {
						t.Fatalf("wanted static transport error containing %q, got %v", tc.want, err)
					}
					after, err := os.ReadFile(path)
					if err != nil || !bytes.Equal(before, after) {
						t.Fatalf("configuration changed: %s (%v)", after, err)
					}
					if requests.Load() != beforeRequests {
						t.Fatal("invalid transport reached Docker")
					}
				})
			}
		}
	}
}

func TestManagementAllowsIncompleteConnectionSettings(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	for _, name := range []string{"setup", "upgrade"} {
		for _, flags := range [][]string{
			nil,
			{"--collector", "localhost:8443"},
			{"--reverse-auth", "mutual-key"},
			{"--reverse-auth", "mutual-key", "--reverse-transport", "http"},
			{"--mode", "inbound"},
			{"--rpc-concurrency", "65536", "--max-message-bytes", strconv.Itoa(plugin.MaxMessageBytes), "--reconnect-reset-after", "0s"},
		} {
			if o, err := executeOptions(t, name, flags...); err != nil || o.out == nil {
				t.Fatalf("%s %v did not reach management: %v", name, flags, err)
			} else if o.transport.name != "" || o.transport.serverName != "" {
				t.Fatal("management populated implicit server identity defaults")
			}
		}
	}
	for _, flags := range [][]string{
		nil,
		{"--collector", "localhost:8443"},
		{"--collector", "localhost:8443", "--reverse-auth", "mutual-key"},
	} {
		if _, err := executeOptions(t, "server", flags...); err == nil {
			t.Fatalf("server accepted incomplete connection settings: %v", flags)
		}
	}
}
