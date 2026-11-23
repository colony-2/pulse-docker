package launchlogs

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	api "github.com/colony-2/pulse-provider-api"
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

func TestArchiveTerminalResetValidatesCommittedTail(t *testing.T) {
	store, _ := archiveFixture(t)
	const id = "one"
	if err := store.Append(id, api.LogSourceStderr, []byte("original")); err != nil {
		t.Fatal(err)
	}
	m, err := store.loadID(id)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(store.root, key(id), m.Segments[0].Name)
	committed, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := bytes.Clone(committed)
	corrupt[len(corrupt)-1] ^= 1
	if err := os.WriteFile(path, corrupt, 0600); err != nil {
		t.Fatal(err)
	}
	store.fail(id, ErrStorage)
	if err := store.Gap(id, "storage_unavailable"); err != nil {
		t.Fatal(err)
	}
	if err := store.ResetAppend(id); !errors.Is(err, ErrStorage) {
		t.Fatal("reset accepted corrupt committed frame", err)
	}
	if err := store.Complete(id); !errors.Is(err, ErrStorage) {
		t.Fatal("completion accepted corrupt committed frame", err)
	}
	if err := store.Append(id, api.LogSourceStderr, []byte("must not append")); !errors.Is(err, ErrStorage) {
		t.Fatal("append failure latch was lost", err)
	}
	if err := os.WriteFile(path, append(bytes.Clone(committed), []byte("uncommitted tail")...), 0600); err != nil {
		t.Fatal(err)
	}
	if err := store.ResetAppend(id); err != nil {
		t.Fatal(err)
	}
	if restored, err := os.ReadFile(path); err != nil || !bytes.Equal(restored, committed) {
		t.Fatal("validated reset did not truncate only the uncommitted tail", err)
	}
	if err := store.Append(id, api.LogSourceStderr, []byte("recovered")); err != nil {
		t.Fatal(err)
	}
	page, err := store.GetLaunchLog(t.Context(), api.LaunchLogRequest{LaunchID: id})
	obs, obsErr := store.Observation(id)
	if err != nil || obsErr != nil || page.Finalized || !page.Incomplete || !slices.Contains(obs.History.GapReasons, "storage_unavailable") || len(page.Records) != 2 || string(page.Records[0].Data) != "original" || string(page.Records[1].Data) != "recovered" || page.Records[1].Sequence != page.Records[0].Sequence+1 {
		t.Fatal(page, err, obs, obsErr)
	}
}

func TestArchiveTerminalMissingMetadataPreservesDurableEvidence(t *testing.T) {
	for _, evidence := range []string{"00000000000000000001.records", "attempt-1234567890abcdef1234567890abcdef.json"} {
		t.Run(evidence, func(t *testing.T) {
			store, config := archiveFixture(t)
			dir := filepath.Join(store.root, key("one"))
			path := filepath.Join(dir, evidence)
			if err := os.WriteFile(path, []byte("durable evidence"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(dir, "metadata.json")); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(config, "daemon"); !errors.Is(err, ErrStorage) {
				t.Fatal("existing records were mistaken for interrupted first write", err)
			}
			if err := store.BeginRecovered(History{LaunchID: "one"}, "abcdef1234567890abcdef1234567890"); !errors.Is(err, ErrStorage) {
				t.Fatal("recovery replaced a launch with durable evidence", err)
			}
			if preserved, err := os.ReadFile(path); err != nil || string(preserved) != "durable evidence" {
				t.Fatal("durable evidence was modified", err)
			}
		})
	}
}

func TestArchiveTerminalResetValidatesSealedTail(t *testing.T) {
	store, _ := archiveFixture(t)
	const id = "one"
	if err := store.Append(id, api.LogSourceStderr, []byte("sealed")); err != nil {
		t.Fatal(err)
	}
	meta, err := store.loadID(id)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.seal(&meta); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(store.root, key(id), meta.Segments[0].Name)
	committed, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := bytes.Clone(committed)
	corrupt[len(corrupt)-1] ^= 1
	if err := os.WriteFile(path, corrupt, 0600); err != nil {
		t.Fatal(err)
	}
	if err := store.ResetAppend(id); !errors.Is(err, ErrStorage) {
		t.Fatal("reset accepted corrupt gzip trailer", err)
	}
	if err := os.WriteFile(path, committed, 0600); err != nil {
		t.Fatal(err)
	}
	if err := store.ResetAppend(id); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(id, api.LogSourceStderr, []byte("next segment")); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, committed) {
		t.Fatal("resumed append modified the sealed segment", err)
	}
	page, err := store.GetLaunchLog(t.Context(), api.LaunchLogRequest{LaunchID: id})
	if err != nil || len(page.Records) != 2 || string(page.Records[0].Data) != "sealed" || string(page.Records[1].Data) != "next segment" || page.Records[1].Sequence != page.Records[0].Sequence+1 {
		t.Fatal(page, err)
	}
}

func TestArchiveTerminalCommitAndCompressionFailure(t *testing.T) {
	store, config := archiveFixture(t)
	exit := 7
	if err := store.RecordProcess("one", Process{State: "exited", ExitCode: &exit, Source: "inspect", ObservedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append("one", api.LogSourceStderr, []byte("exact stderr")); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(store.root, key("one"))
	meta, err := load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.stage(&meta, api.LogSourceLaunch, []byte("uncommitted terminal frame"), time.Time{}); err != nil {
		t.Fatal(err)
	}
	store, err = Open(config, "daemon")
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.GetLaunchLog(t.Context(), api.LaunchLogRequest{LaunchID: "one"})
	if err != nil || before.Finalized || len(before.Records) != 1 {
		t.Fatal(before, err)
	}
	if err = os.Mkdir(filepath.Join(dir, meta.Segments[0].Name+".gz"), 0700); err != nil {
		t.Fatal(err)
	}
	if err = store.Complete("one"); err == nil {
		t.Fatal("compression fault was not exercised")
	}
	store, err = Open(config, "daemon")
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Complete("one"); err != nil {
		t.Fatal(err)
	}
	after, err := store.GetLaunchLog(t.Context(), api.LaunchLogRequest{LaunchID: "one", PageToken: before.NextPageToken})
	if err != nil || !after.Finalized || !after.Incomplete || len(after.Records) != 1 || !bytes.Contains(after.Records[0].Data, []byte("code=7")) {
		t.Fatal(after, err)
	}
	obs, err := store.DurableObservation("one")
	if err != nil || !obs.Finalized || *obs.History.Process.ExitCode != exit {
		t.Fatal(obs, err)
	}
}

func TestArchiveTerminalReceiptSurvivesEviction(t *testing.T) {
	store, _ := archiveFixture(t)
	receipt := Receipt{Container: "container", Fingerprint: "fingerprint", Scope: "owner", Phase: "accepted"}
	if err := store.RecordReceipt("one", receipt); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordProcess("one", Process{State: "unknown", Reason: "container_missing", ObservedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := store.Complete("one"); err != nil {
		t.Fatal(err)
	}
	store.cfg.MaxAge = time.Nanosecond
	if err := store.Clean(); err != nil {
		t.Fatal(err)
	}
	obs, err := store.DurableObservation("one")
	if err != nil || obs.Receipt == nil || *obs.Receipt != receipt || !obs.Finalized || obs.History.Process.Reason != "container_missing" || obs.History.Capture != "expired" {
		t.Fatal(obs, err)
	}
	if _, err := store.GetLaunchLog(t.Context(), api.LaunchLogRequest{LaunchID: "one"}); !errors.Is(err, api.ErrLaunchLogNotFound) {
		t.Fatal(err)
	}
}

func TestArchiveTerminalAdmissionConflictPreservesActiveLaunch(t *testing.T) {
	store, _ := archiveFixture(t)
	if err := store.RecordReceipt("one", Receipt{Phase: "accepted"}); err != nil {
		t.Fatal(err)
	}
	for _, status := range []string{"accepted", "accepted", "rejected", "unknown"} {
		if err := store.RecordAdmission("one", status, status == "rejected", false); err != nil {
			t.Fatal(err)
		}
	}
	obs, err := store.Observation("one")
	if err != nil || obs.Finalized || obs.History.Admission != "accepted" {
		t.Fatal(obs, err)
	}
	page, err := store.GetLaunchLog(t.Context(), api.LaunchLogRequest{LaunchID: "one"})
	if err != nil || len(page.Records) != 1 || string(page.Records[0].Data) != "admission: accepted\n" {
		t.Fatal(page, err)
	}
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

func TestArchiveTerminalRetryRetention(t *testing.T) {
	store, _ := archiveFixture(t)
	if err := store.RecordAdmission("one", "rejected", true, false); err != nil {
		t.Fatal(err)
	}
	request := api.LaunchLogRequest{LaunchID: "one"}
	prior, err := store.GetLaunchLog(t.Context(), request)
	if err != nil || !prior.Finalized {
		t.Fatal(prior, err)
	}
	rejected, err := store.loadID("one")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PrepareAttempt(History{LaunchID: "one", SubmittedAt: time.Now()}, Receipt{Phase: "uncertain"}); err != nil {
		t.Fatal(err)
	}
	older, err := loadMetadata(filepath.Join(store.root, key("one")), snapshotName(rejected.Archive))
	if err != nil {
		t.Fatal(err)
	}
	older.ClosedAt = time.Now().Add(-2 * store.cfg.MaxAge)
	if err := store.saveMetadata(older, snapshotName(older.Archive)); err != nil {
		t.Fatal(err)
	}
	if err := store.Append("one", api.LogSourceStderr, []byte("retry stderr")); err != nil {
		t.Fatal(err)
	}
	page, err := store.GetLaunchLog(t.Context(), request)
	if err != nil || page.Finalized || len(page.Records) != 1 || string(page.Records[0].Data) != "retry stderr" {
		t.Fatal(page, err)
	}
	request.PageToken = prior.NextPageToken
	if _, err := store.GetLaunchLog(t.Context(), request); !errors.Is(err, api.ErrLaunchLogNotFound) {
		t.Fatal("expired attempt cursor was not rejected", err)
	}
	for _, part := range older.Segments {
		if _, err := os.Stat(filepath.Join(store.root, key("one"), part.Name)); !os.IsNotExist(err) {
			t.Fatal("expired attempt segment not removed", part.Name, err)
		}
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
