package launchlogs

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"

	api "github.com/colony-2/pulse-provider-api"
)

type Process struct {
	State                 string
	ExitCode              *int       `json:",omitempty"`
	OOMKilled             *bool      `json:",omitempty"`
	StartedAt, FinishedAt *time.Time `json:",omitempty"`
	ObservedAt            time.Time
	Source, Reason        string `json:",omitempty"`
}

func (p *Process) Display() (state, exit, oom, reason string) {
	state, exit, oom, reason = "unknown", "unknown", "unknown", "legacy_unknown"
	if p == nil {
		return
	}
	state, reason = p.State, p.Reason
	if reason == "" {
		reason = "-"
	}
	if p.State == "exited" && p.ExitCode != nil {
		exit = fmt.Sprint(*p.ExitCode)
	}
	if p.OOMKilled != nil {
		oom = fmt.Sprint(*p.OOMKilled)
	}
	return
}

type Receipt struct {
	Container, Fingerprint, Scope, Phase string
	Cleanup                              string     `json:",omitempty"`
	StoppedAt                            *time.Time `json:",omitempty"`
}

type Observation struct {
	History   History
	Archive   string
	Receipt   *Receipt
	Finalized bool
}

func (s *Store) Observation(id string) (Observation, error) {
	return s.observation(id, false)
}

func (s *Store) DurableObservation(id string) (Observation, error) {
	return s.observation(id, true)
}

func (s *Store) observation(id string, durable bool) (Observation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, err := s.loadID(id)
	if err == nil && durable {
		err = errors.Join(syncDir(filepath.Join(s.root, key(id))), syncDir(s.root))
	}
	return Observation{m.History, m.Archive, m.Receipt, m.Finalized}, err
}

func (s *Store) loadID(id string) (metadata, error) {
	m, err := load(filepath.Join(s.root, key(id)))
	if err == nil && m.ID != id {
		return metadata{}, ErrStorage
	}
	return m, err
}

func (s *Store) update(id string, update func(*metadata)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, err := s.loadID(id)
	if err != nil {
		return err
	}
	update(&m)
	return s.save(m)
}

func (s *Store) RecordReceipt(id string, receipt Receipt) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, err := s.loadID(id)
	if err != nil {
		return err
	}
	if m.Finalized && m.Receipt == nil && m.History.ContainerID != receipt.Container {
		return ErrStorage
	}
	m.Receipt = &receipt
	if receipt.Container != "" {
		m.History.ContainerID = receipt.Container
	}
	return s.save(m)
}

func (s *Store) PrepareAttempt(history History, receipt Receipt) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := history.LaunchID
	m, err := s.loadID(id)
	if err != nil {
		return "", err
	}
	if !m.Finalized {
		m.Receipt = &receipt
		return m.Archive, s.save(m)
	}
	archive, err := archiveNonce()
	if err != nil {
		return "", err
	}
	if !validArchive.MatchString(m.Archive) || m.Receipt != nil || m.History.ContainerID != "" || m.TerminalSequence != 0 ||
		m.History.Admission == "" || m.History.Admission == "unknown" || m.History.Admission == "accepted" {
		return archive, ErrStorage
	}
	if err := s.saveMetadata(m, snapshotName(m.Archive)); err != nil {
		return archive, err
	}
	history.Capture = "preparing"
	next := metadata{Version: 1, ID: id, Archive: archive, Next: 1, Updated: history.SubmittedAt, History: history, Receipt: &receipt, HasPriorAttempts: true}
	if err := s.save(next); err != nil {
		return archive, err
	}
	delete(s.failed, id)
	return archive, nil
}

func (s *Store) ClearReceipt(id string) error {
	return s.update(id, func(m *metadata) { m.Receipt = nil })
}

func (s *Store) RecordAdmission(id, admission string, final, missing bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, err := s.loadID(id)
	if err != nil || m.Finalized || m.History.Admission == admission {
		return err
	}
	if m.Receipt != nil && (final || admission != "unknown" && admission != m.Receipt.Phase || m.Receipt.Phase == "accepted" && admission != "accepted") {
		return nil
	}
	if admission != "" {
		m.History.Admission = admission
		if s.failed[id] == nil {
			data := []byte("admission: " + admission + "\n")
			if err := s.stage(&m, api.LogSourceLaunch, data, time.Time{}); err != nil {
				return s.fail(id, err)
			}
		}
	}
	if missing {
		gap(&m, "admission_unavailable")
	}
	if final {
		m.Finalized = true
		m.ClosedAt = time.Now().UTC()
		m.History.Capture = "closed"
	}
	if err := s.save(m); err != nil {
		return s.fail(id, err)
	}
	if final && s.cfg.Compress && len(m.Segments) != 0 {
		return s.seal(&m)
	}
	return nil
}

func (s *Store) RecordProcess(id string, process Process) error {
	return s.update(id, func(m *metadata) {
		if prior := m.History.Process; prior != nil && prior.State == "exited" {
			if process.State != "exited" && process.Reason != "observation_conflict" {
				return
			}
			if process.ExitCode != nil && prior.ExitCode != nil && *process.ExitCode == *prior.ExitCode {
				if process.StartedAt == nil {
					process.StartedAt = prior.StartedAt
				}
				if process.FinishedAt == nil {
					process.FinishedAt = prior.FinishedAt
				}
				if process.OOMKilled == nil {
					process.OOMKilled = prior.OOMKilled
					if process.State == "exited" && process.OOMKilled != nil && *process.OOMKilled {
						process.Reason = "oom_killed"
					}
				}
			}
		}
		m.History.Process = &process
	})
}

func gap(m *metadata, reason string) {
	m.Incomplete = true
	m.History.Incomplete = true
	for _, old := range m.History.GapReasons {
		if old == reason {
			return
		}
	}
	if len(m.History.GapReasons) < 16 {
		m.History.GapReasons = append(m.History.GapReasons, reason)
	}
}

func (s *Store) Gap(id, reason string) error {
	return s.update(id, func(m *metadata) { gap(m, reason) })
}

func (s *Store) ResetAppend(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, err := s.loadID(id)
	if err != nil {
		return err
	}
	if m.Deleted || m.Finalized {
		return ErrStorage
	}
	if slices.Contains(m.History.GapReasons, "archive_budget") {
		if err := s.cleanLocked(); err != nil {
			return err
		}
	}
	if err := s.validateAppendTail(m); err != nil {
		return s.fail(id, err)
	}
	delete(s.failed, id)
	return nil
}

func (s *Store) validateAppendTail(observation metadata) error {
	if len(observation.Segments) == 0 {
		return nil
	}
	tail := observation.Segments[len(observation.Segments)-1]
	path := filepath.Join(s.root, key(observation.ID), tail.Name)
	stat, err := os.Lstat(path)
	if err != nil || !stat.Mode().IsRegular() || stat.Size() < tail.Size {
		return ErrStorage
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	reader := io.LimitReader(file, tail.Size)
	compressed := filepath.Ext(tail.Name) == ".gz"
	if compressed {
		if stat.Size() != tail.Size {
			return ErrStorage
		}
		decoded, err := gzip.NewReader(reader)
		if err != nil {
			return err
		}
		defer decoded.Close()
		reader = decoded
	}
	for sequence := tail.First; sequence < tail.Next; sequence++ {
		if _, err := readFrame(reader, sequence); err != nil {
			return err
		}
	}
	var extra [1]byte
	if _, err := io.ReadFull(reader, extra[:]); err != io.EOF {
		return ErrStorage
	}
	if !compressed && stat.Size() > tail.Size {
		return os.Truncate(path, tail.Size)
	}
	return nil
}

func (s *Store) Complete(id string) error {
	return s.complete(id, "")
}

func (s *Store) CompleteExpired(id string) error {
	return s.complete(id, "native_retention_expired")
}

func (s *Store) CompleteMissingContainer(id string) error {
	return s.complete(id, "container_missing")
}

func (s *Store) complete(id, unavailableReason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, err := s.loadID(id)
	if err != nil || m.Finalized {
		return err
	}
	if unavailableReason != "" {
		gap(&m, unavailableReason)
	}
	if s.failed[id] != nil {
		gap(&m, "storage_unavailable")
	}
	if err := s.validateAppendTail(m); err != nil {
		gap(&m, "storage_unavailable")
		if unavailableReason != "" {
			closeCapture(&m)
			return s.save(m)
		}
		return errors.Join(s.save(m), s.fail(id, err))
	}
	state, exit, oom, reason := m.History.Process.Display()
	summary := fmt.Sprintf("Docker worker exit unknown: reason=%s", reason)
	if state == "exited" {
		summary = fmt.Sprintf("Docker worker exited: code=%s, oom_killed=%s", exit, oom)
	}
	if m.History.Process != nil && m.History.Process.FinishedAt != nil {
		summary += ", finished_at=" + m.History.Process.FinishedAt.Format(time.RFC3339Nano)
	}
	if len(m.History.GapReasons) > 0 {
		summary += fmt.Sprintf("; capture gaps: %v", m.History.GapReasons)
	}
	if err := s.stage(&m, api.LogSourceLaunch, []byte(summary+"\n"), time.Time{}); err != nil {
		if unavailableReason == "" {
			return s.fail(id, err)
		}
		committed, loadErr := s.loadID(id)
		if loadErr != nil {
			return errors.Join(s.fail(id, err), loadErr)
		}
		gap(&committed, unavailableReason)
		gap(&committed, "storage_unavailable")
		closeCapture(&committed)
		return s.save(committed)
	}
	m.TerminalSequence = m.Next - 1
	closeCapture(&m)
	if err := s.save(m); err != nil {
		return s.fail(id, err)
	}
	if s.cfg.Compress {
		return s.seal(&m)
	}
	return nil
}

func closeCapture(observation *metadata) {
	observation.Finalized = true
	observation.ClosedAt = time.Now().UTC()
	observation.History.Capture = "closed"
}
