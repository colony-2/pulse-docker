package launchlogs

import (
	"bytes"
	"context"
	"errors"
	api "github.com/colony-2/pulse-provider-api"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func archiveFixture(t *testing.T) (*Store, Config) {
	t.Helper()
	c := Config{Directory: t.TempDir(), Owner: "test", Compress: true}
	s, err := Open(c, "daemon")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Begin(History{LaunchID: "one", TenantID: "tenant", JobID: "job", SubmittedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	return s, c
}
func TestArchiveBinaryPagingReopen(t *testing.T) {
	s, c := archiveFixture(t)
	raw := append([]byte{0, 255, 128}, bytes.Repeat([]byte("x"), 8192)...)
	if err := s.Append("one", api.LogSourceStdout, raw); err != nil {
		t.Fatal(err)
	}
	if err := s.Finish("one", true, false); err != nil {
		t.Fatal(err)
	}
	p, err := s.GetLaunchLog(t.Context(), api.LaunchLogRequest{LaunchID: "one", PageSize: 1})
	if err != nil || len(p.Records) != 1 || !p.HasMore || !p.Finalized || p.Incomplete || p.NextPageToken == "" {
		t.Fatal(p, err)
	}
	token := p.NextPageToken
	head := append([]byte(nil), p.Records[0].Data...)
	p.Records[0].Data[0] = 42
	s, err = Open(c, "daemon")
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.GetLaunchLog(t.Context(), api.LaunchLogRequest{LaunchID: "one", PageSize: 1})
	if err != nil || !bytes.Equal(again.Records[0].Data, head) {
		t.Fatal(again, err)
	}
	tail, err := s.GetLaunchLog(t.Context(), api.LaunchLogRequest{LaunchID: "one", PageToken: token, PageSize: 100})
	if err != nil || tail.HasMore || len(tail.Records) != 2 {
		t.Fatal(tail, err)
	}
	got := head
	for _, r := range tail.Records {
		got = append(got, r.Data...)
	}
	if !bytes.Equal(got, raw) {
		t.Fatal("binary data changed")
	}
	empty, err := s.GetLaunchLog(t.Context(), api.LaunchLogRequest{LaunchID: "one", PageToken: tail.NextPageToken})
	if err != nil || empty.Records == nil || len(empty.Records) != 0 || empty.HasMore || empty.NextPageToken == "" {
		t.Fatal(empty, err)
	}
	if err = s.Append("one", api.LogSourceStdout, []byte("late")); err == nil {
		t.Fatal("appended to finalized archive")
	}
	m, _ := load(filepath.Join(s.root, key("one")))
	if filepath.Ext(m.Segments[0].Name) != ".gz" {
		t.Fatal("sealed archive not compressed")
	}
}
func TestArchiveCursorCategoriesAndIsolation(t *testing.T) {
	s, _ := archiveFixture(t)
	s.Append("one", api.LogSourceStderr, []byte("a"))
	page, _ := s.GetLaunchLog(t.Context(), api.LaunchLogRequest{LaunchID: "one"})
	other, err := Open(Config{Directory: s.cfg.Directory, Owner: "other"}, "daemon")
	if err != nil {
		t.Fatal(err)
	}
	other.Begin(History{LaunchID: "one"})
	for _, tc := range []struct {
		s    *Store
		q    api.LaunchLogRequest
		want error
	}{{s, api.LaunchLogRequest{LaunchID: "../one"}, api.ErrLaunchLogInvalidRequest}, {s, api.LaunchLogRequest{LaunchID: "one", PageSize: -1}, api.ErrLaunchLogInvalidRequest}, {s, api.LaunchLogRequest{LaunchID: "one", PageSize: 101}, api.ErrLaunchLogInvalidRequest}, {s, api.LaunchLogRequest{LaunchID: "absent", PageToken: "bad"}, api.ErrLaunchLogNotFound}, {s, api.LaunchLogRequest{LaunchID: "one", PageToken: "bad"}, api.ErrLaunchLogCursor}, {s, api.LaunchLogRequest{LaunchID: "one", PageToken: string(bytes.Repeat([]byte("a"), 16385))}, api.ErrLaunchLogCursor}, {other, api.LaunchLogRequest{LaunchID: "one", PageToken: page.NextPageToken}, api.ErrLaunchLogCursor}} {
		if _, err = tc.s.GetLaunchLog(t.Context(), tc.q); !errors.Is(err, tc.want) {
			t.Fatalf("%+v: %v wanted %v", tc.q, err, tc.want)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err = s.GetLaunchLog(ctx, api.LaunchLogRequest{LaunchID: "one"}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for _, path := range []string{s.root, filepath.Join(s.root, key("one")), filepath.Join(s.root, key("one"), "metadata.json")} {
		st, err := os.Stat(path)
		if err != nil || st.Mode().Perm()&0077 != 0 {
			t.Fatal(path, st, err)
		}
	}
}
func TestArchiveTornTailAndCommittedCorruption(t *testing.T) {
	s, c := archiveFixture(t)
	s.Append("one", api.LogSourceStdout, []byte("committed"))
	m, _ := load(filepath.Join(s.root, key("one")))
	path := filepath.Join(s.root, key("one"), m.Segments[0].Name)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte("torn"))
	f.Close()
	s, err = Open(c, "daemon")
	if err != nil {
		t.Fatal(err)
	}
	p, err := s.GetLaunchLog(t.Context(), api.LaunchLogRequest{LaunchID: "one"})
	if err != nil || !p.Incomplete || string(p.Records[0].Data) != "committed" {
		t.Fatal(p, err)
	}
	f, err = os.OpenFile(path, os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteAt([]byte{255}, 10)
	f.Close()
	if _, err = s.GetLaunchLog(t.Context(), api.LaunchLogRequest{LaunchID: "one"}); !errors.Is(err, ErrStorage) {
		t.Fatal("corruption hidden", err)
	}
}
func TestArchiveRetentionAndHistory(t *testing.T) {
	s, _ := archiveFixture(t)
	s.Append("one", api.LogSourceStdout, []byte("old"))
	p, _ := s.GetLaunchLog(t.Context(), api.LaunchLogRequest{LaunchID: "one"})
	s.Finish("one", true, false)
	s.cfg.MaxAge = time.Nanosecond
	if err := s.Clean(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetLaunchLog(t.Context(), api.LaunchLogRequest{LaunchID: "one", PageToken: p.NextPageToken}); !errors.Is(err, api.ErrLaunchLogNotFound) {
		t.Fatal(err)
	}
	h, err := s.Histories()
	if err != nil || len(h) != 1 || h[0].Capture != "expired" || !h[0].Incomplete {
		t.Fatal(h, err)
	}
	s.Begin(History{LaunchID: "active"})
	s.cfg.MaxBytes = 1
	if err = s.Append("active", api.LogSourceStdout, []byte("overflow")); !errors.Is(err, ErrBudget) {
		t.Fatal(err)
	}
	if err = s.Finish("active", true, true); err != nil {
		t.Fatal(err)
	}
	page, err := s.GetLaunchLog(t.Context(), api.LaunchLogRequest{LaunchID: "active"})
	if err != nil || !page.Incomplete || !page.Finalized {
		t.Fatal(page, err)
	}
}
func TestArchiveBackendFailureIsNotUnsupported(t *testing.T) {
	s, _ := archiveFixture(t)
	dir := filepath.Join(s.root, key("one"))
	if err := os.Remove(filepath.Join(dir, "metadata.json")); err != nil {
		t.Fatal(err)
	}
	os.Mkdir(filepath.Join(dir, "metadata.json"), 0700)
	if _, err := s.GetLaunchLog(t.Context(), api.LaunchLogRequest{LaunchID: "one"}); err == nil || errors.Is(err, api.ErrLaunchLogNotFound) || errors.Is(err, api.ErrLaunchLogUnsupported) {
		t.Fatal(err)
	}
	if err := s.Append("one", api.LogSourceStdout, []byte("x")); err == nil {
		t.Fatal("failed storage accepted append")
	}
}
func TestArchiveRejectsSymlinkedPrivateMetadata(t *testing.T) {
	s, _ := archiveFixture(t)
	dir := filepath.Join(s.root, key("one"))
	original, err := os.ReadFile(filepath.Join(dir, "metadata.json"))
	if err != nil {
		t.Fatal(err)
	}
	foreign := filepath.Join(t.TempDir(), "metadata")
	os.WriteFile(foreign, original, 0600)
	os.Remove(filepath.Join(dir, "metadata.json"))
	if err = os.Symlink(foreign, filepath.Join(dir, "metadata.json")); err != nil {
		t.Fatal(err)
	}
	if _, err = s.GetLaunchLog(t.Context(), api.LaunchLogRequest{LaunchID: "one"}); !errors.Is(err, ErrStorage) {
		t.Fatal("followed foreign metadata", err)
	}
}
