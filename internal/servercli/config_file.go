package servercli

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type savedConfiguration struct {
	Version int               `json:"version"`
	Flags   map[string]string `json:"flags"`
}

func persistedFlag(name string) bool {
	switch name {
	case "config", "timeout", "progress", "help", "version", "json", "tenant", "job", "since", "limit", "follow", "offset", "launch-id":
		return false
	}
	return true
}
func pathFlag(name string) bool {
	switch name {
	case "log-directory", "lock-dir", "scratch-path", "executor-env-file", "registry-auth-file", "legacy-endpoints-file", "tls-ca", "tls-cert", "tls-key", "shared-key-file":
		return true
	}
	return false
}

// Reject duplicates recursively before decoding typed values. encoding/json's
// ordinary last-key-wins behavior would conceal conflicting secret selectors.
func strictJSON(b []byte, out any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	var value func() error
	value = func() error {
		tok, err := d.Token()
		if err != nil {
			return err
		}
		if delim, ok := tok.(json.Delim); ok {
			switch delim {
			case '{':
				seen := map[string]bool{}
				for d.More() {
					key, err := d.Token()
					if err != nil {
						return err
					}
					s, ok := key.(string)
					if !ok || seen[s] {
						return fmt.Errorf("duplicate JSON key")
					}
					seen[s] = true
					if err = value(); err != nil {
						return err
					}
				}
				end, err := d.Token()
				if err != nil || end != json.Delim('}') {
					return fmt.Errorf("invalid JSON object")
				}
			case '[':
				for d.More() {
					if err = value(); err != nil {
						return err
					}
				}
				end, err := d.Token()
				if err != nil || end != json.Delim(']') {
					return fmt.Errorf("invalid JSON array")
				}
			default:
				return fmt.Errorf("invalid JSON delimiter")
			}
		}
		return nil
	}
	if err := value(); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return fmt.Errorf("extra JSON values")
	}
	d = json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	return d.Decode(out)
}
func loadConfiguration(cmd *cobra.Command, o *serverOptions) error {
	explicit := cmd.Flags().Changed("config")
	if o.configPath == "" {
		h, err := os.UserHomeDir()
		if err != nil {
			return err
		}
		o.configPath = filepath.Join(h, ".pulse", "docker", "provider.json")
	}
	var err error
	o.configPath, err = absolutePath(o.configPath)
	if err != nil {
		return err
	}
	b, err := readFile(o.configPath, 1<<20)
	if os.IsNotExist(err) && (!explicit || cmd.Name() == "setup") {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read configuration: %w", err)
	}
	var envelope map[string]json.RawMessage
	if strictJSON(b, &envelope) != nil || len(envelope) != 2 || envelope["version"] == nil || envelope["flags"] == nil {
		return fmt.Errorf("configuration requires version and flags")
	}
	var values map[string]json.RawMessage
	if json.Unmarshal(envelope["flags"], &values) != nil || values == nil {
		return fmt.Errorf("configuration flags must be strings")
	}
	for _, v := range values {
		if len(v) == 0 || v[0] != '"' {
			return fmt.Errorf("configuration flags must be strings")
		}
	}
	var saved savedConfiguration
	if strictJSON(b, &saved) != nil || saved.Version != 1 || saved.Flags == nil {
		return fmt.Errorf("configuration requires one version 1 flags object with unique known keys")
	}
	for name, value := range saved.Flags {
		f := cmd.LocalNonPersistentFlags().Lookup(name)
		if f == nil || !persistedFlag(name) {
			return fmt.Errorf("unsupported saved option --%s", name)
		}
		if f.Changed {
			continue
		}
		if pathFlag(name) && value != "" && !filepath.IsAbs(value) && !strings.HasPrefix(value, "~") {
			value = filepath.Join(filepath.Dir(o.configPath), value)
		}
		if err = f.Value.Set(value); err != nil {
			return fmt.Errorf("invalid saved option --%s", name)
		}
	}
	return nil
}
func saveConfiguration(cmd *cobra.Command, path string) error {
	saved := savedConfiguration{Version: 1, Flags: map[string]string{}}
	cmd.LocalNonPersistentFlags().VisitAll(func(f *pflag.Flag) {
		if persistedFlag(f.Name) {
			saved.Flags[f.Name] = f.Value.String()
		}
	})
	b, err := json.MarshalIndent(saved, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err = os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".provider-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(append(b, '\n')); err == nil {
		err = f.Sync()
	}
	if e := f.Close(); err == nil {
		err = e
	}
	if err != nil {
		return err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	fmt.Fprintln(cmd.ErrOrStderr(), "Configuration saved:", path)
	return nil
}
func readPrivateFile(path string, limit int64) ([]byte, error) {
	st, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if st.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("secret file permissions must exclude group and other access")
	}
	return readFile(path, limit)
}
func bytesTrimLine(b []byte) []byte {
	if bytes.HasSuffix(b, []byte("\n")) {
		b = bytes.TrimSuffix(b, []byte("\n"))
		b = bytes.TrimSuffix(b, []byte("\r"))
	}
	return b
}
func validateRegistryAuth(b []byte) error {
	b = bytesTrimLine(b)
	decoded, err := base64.URLEncoding.DecodeString(string(b))
	if err != nil {
		decoded, err = base64.RawURLEncoding.DecodeString(string(b))
	}
	var auth map[string]string
	if err != nil || strictJSON(decoded, &auth) != nil || len(auth) == 0 {
		return fmt.Errorf("invalid encoded registry auth file")
	}
	return nil
}
func (o transportOptions) tlsConfigForMode(mode string) (*tls.Config, error) {
	if mode == "inbound" {
		return nil, nil
	}
	return o.tlsConfig()
}

func stringMapJSON(b []byte, out *map[string]string) error {
	var raw map[string]json.RawMessage
	if strictJSON(b, &raw) != nil || raw == nil {
		return fmt.Errorf("expected a string map")
	}
	for _, v := range raw {
		if len(v) == 0 || v[0] != '"' {
			return fmt.Errorf("expected string values")
		}
	}
	return strictJSON(b, out)
}
