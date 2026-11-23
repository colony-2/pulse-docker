// Package launchlogs stores observations, never execution or replay authority.
package launchlogs

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"time"

	api "github.com/colony-2/pulse-provider-api"
)

var validID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)
var validArchive = regexp.MustCompile(`^[a-f0-9]{32}$`)
var attemptSnapshot = regexp.MustCompile(`^attempt-[a-f0-9]{32}\.json$`)
var stagingFile = regexp.MustCompile(`^\.commit-[0-9]+$`)
var launchDirectory = regexp.MustCompile(`^[a-f0-9]{64}$`)
var ErrStorage = errors.New("launch archive unavailable or corrupt")
var ErrBudget = errors.New("launch archive byte budget exhausted")

const segmentBytes = 16 << 20

type Config struct {
	Directory, Owner string
	MaxAge           time.Duration
	MaxBytes         int64
	Compress         bool
}

func (c Config) Normalize() (Config, error) {
	if c.Directory == "" || !filepath.IsAbs(c.Directory) || !validID.MatchString(c.Owner) {
		return c, fmt.Errorf("logging requires an absolute directory and a valid owner")
	}
	if c.MaxAge == 0 {
		c.MaxAge = 7 * 24 * time.Hour
	}
	if c.MaxBytes == 0 {
		c.MaxBytes = 1 << 30
	}
	if c.MaxAge < 0 || c.MaxBytes < 1 {
		return c, fmt.Errorf("invalid log retention")
	}
	return c, nil
}
func Scope(directory, daemon, owner string) string {
	sum := sha256.Sum256([]byte("docker-archive-v1\x00" + daemon + "\x00" + owner))
	return filepath.Join(directory, hex.EncodeToString(sum[:]))
}
func key(id string) string { h := sha256.Sum256([]byte(id)); return hex.EncodeToString(h[:]) }

// History deliberately excludes leases, endpoints, commands and environment.
type History struct {
	LaunchID, InstanceID, TenantID, JobID, ContainerID, Admission, ImageID string
	SubmittedAt                                                            time.Time
	Capture                                                                string
	Incomplete                                                             bool
	Process                                                                *Process `json:",omitempty"`
	GapReasons                                                             []string `json:",omitempty"`
}
type segment struct {
	Name        string
	First, Next uint64
	Size        int64
}
type metadata struct {
	NativeTimestamp                time.Time
	NativeFingerprint              string
	NativeSequence                 uint64
	Version                        int
	ID, Archive                    string
	Next                           uint64
	Bytes                          int64
	Segments                       []segment
	Finalized, Incomplete, Deleted bool
	Updated, ClosedAt              time.Time
	History                        History
	Receipt                        *Receipt `json:",omitempty"`
	TerminalSequence               uint64   `json:",omitempty"`
	HasPriorAttempts               bool     `json:",omitempty"`
}
type Store struct {
	mu     sync.Mutex
	root   string
	cfg    Config
	failed map[string]error
}

func Open(c Config, daemon string) (*Store, error) {
	c, err := c.Normalize()
	if err != nil {
		return nil, err
	}
	s := &Store{root: Scope(c.Directory, daemon, c.Owner), cfg: c, failed: map[string]error{}}
	if err = privateDir(s.root); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		m, err := load(filepath.Join(s.root, e.Name()))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if key(m.ID) != e.Name() {
			return nil, ErrStorage
		}
		if !m.Deleted && len(m.Segments) > 0 {
			last := m.Segments[len(m.Segments)-1]
			if filepath.Ext(last.Name) != ".gz" {
				path := filepath.Join(s.root, e.Name(), last.Name)
				st, err := os.Stat(path)
				if err != nil {
					return nil, ErrStorage
				}
				if st.Size() < last.Size {
					return nil, ErrStorage
				}
				if st.Size() > last.Size {
					if err = os.Truncate(path, last.Size); err != nil {
						return nil, err
					}
					m.Incomplete = true
					m.History.Incomplete = true
					if err = s.save(m); err != nil {
						return nil, err
					}
				}
			}
		}
	}
	err = s.Clean()
	if errors.Is(err, ErrBudget) {
		err = nil
	}
	return s, err
}
func privateDir(path string) error {
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	st, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 || st.Mode().Perm()&0077 != 0 {
		return ErrStorage
	}
	return nil
}
func atomic(dir, name string, data []byte) error {
	if err := privateDir(dir); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".commit-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	if err := os.Rename(f.Name(), filepath.Join(dir, name)); err != nil {
		return err
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}
func load(dir string) (metadata, error) {
	current, err := loadMetadata(dir, "metadata.json")
	if !os.IsNotExist(err) {
		return current, err
	}
	entries, readErr := os.ReadDir(dir)
	if os.IsNotExist(readErr) {
		return current, err
	}
	if readErr != nil || !launchDirectory.MatchString(filepath.Base(dir)) {
		return metadata{}, ErrStorage
	}
	for _, entry := range entries {
		if entry.Name() == "metadata.json" {
			return loadMetadata(dir, "metadata.json")
		}
	}
	for _, entry := range entries {
		if !stagingFile.MatchString(entry.Name()) {
			return metadata{}, ErrStorage
		}
		info, statErr := entry.Info()
		if os.IsNotExist(statErr) {
			continue
		}
		if statErr != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
			return metadata{}, ErrStorage
		}
	}
	return current, err
}

func loadMetadata(dir, name string) (metadata, error) {
	var m metadata
	st, err := os.Lstat(dir)
	if err != nil {
		return m, err
	}
	if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 || st.Mode().Perm()&0077 != 0 {
		return m, ErrStorage
	}
	st, err = os.Lstat(filepath.Join(dir, name))
	if err != nil {
		return m, err
	}
	if !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
		return m, ErrStorage
	}
	f, err := os.Open(filepath.Join(dir, name))
	if err != nil {
		return m, err
	}
	defer f.Close()
	err = json.NewDecoder(io.LimitReader(f, 4<<20)).Decode(&m)
	if err != nil || m.Version != 1 || !validID.MatchString(m.ID) || m.Archive == "" || (m.HasPriorAttempts && !validArchive.MatchString(m.Archive)) || m.Next < 1 || m.Bytes < 0 {
		return m, ErrStorage
	}
	for i, s := range m.Segments {
		if filepath.Base(s.Name) != s.Name || s.First < 1 || s.Next < s.First || s.Size < 0 || (i > 0 && m.Segments[i-1].Next != s.First) {
			return m, ErrStorage
		}
	}
	if len(m.Segments) == 0 && m.Next != 1 || len(m.Segments) > 0 && m.Segments[len(m.Segments)-1].Next != m.Next {
		return m, ErrStorage
	}
	return m, nil
}
func (s *Store) save(m metadata) error {
	return s.saveMetadata(m, "metadata.json")
}

func (s *Store) saveMetadata(m metadata, name string) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if err := atomic(filepath.Join(s.root, key(m.ID)), name, b); err != nil {
		return err
	}
	return syncDir(s.root)
}

func archiveNonce() (string, error) {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	return hex.EncodeToString(nonce), nil
}

func snapshotName(archive string) string { return "attempt-" + archive + ".json" }
func (s *Store) Begin(h History) error {
	return s.BeginRecovered(h, "")
}

func (s *Store) BeginRecovered(h History, archive string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !validID.MatchString(h.LaunchID) {
		return api.ErrLaunchLogInvalidRequest
	}
	if _, err := load(filepath.Join(s.root, key(h.LaunchID))); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	if archive == "" {
		var err error
		if archive, err = archiveNonce(); err != nil {
			return err
		}
	}
	h.Capture = "preparing"
	m := metadata{Version: 1, ID: h.LaunchID, Archive: archive, Next: 1, Updated: time.Now().UTC(), History: h}
	return s.save(m)
}
func (s *Store) Observe(id, container, admission, image string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, err := load(filepath.Join(s.root, key(id)))
	if err != nil {
		return err
	}
	if m.Finalized {
		return nil
	}
	if container != "" {
		m.History.ContainerID = container
	}
	if admission != "" {
		m.History.Admission = admission
	}
	if image != "" {
		m.History.ImageID = image
	}
	return s.save(m)
}
func (s *Store) Append(id string, source api.LogSource, data []byte) error {
	return s.appendRecord(id, source, data, time.Time{})
}
func (s *Store) AppendNative(id string, source api.LogSource, data []byte, nativeTime time.Time) error {
	return s.appendRecord(id, source, data, nativeTime)
}
func (s *Store) appendRecord(id string, source api.LogSource, data []byte, nativeTime time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.failed[id]; err != nil {
		return err
	}
	m, err := load(filepath.Join(s.root, key(id)))
	if err != nil {
		return err
	}
	if m.Finalized || m.Deleted {
		if source == api.LogSourceLaunch {
			return nil
		}
		return ErrStorage
	}
	for len(data) > 0 {
		n := min(len(data), api.MaxLogRecordBytes)
		if err = s.stage(&m, source, data[:n], nativeTime); err != nil {
			return s.fail(id, err)
		}
		if err = s.save(m); err != nil {
			return s.fail(id, err)
		}
		data = data[n:]
	}
	if err = s.cleanLocked(); err != nil {
		return s.fail(id, err)
	}
	return nil
}

func (s *Store) stage(m *metadata, source api.LogSource, data []byte, nativeTime time.Time) error {
	r := api.LaunchLogRecord{Sequence: m.Next, Timestamp: time.Now().UTC(), Source: source, Data: data}
	payload, _ := json.Marshal(r)
	var frame bytes.Buffer
	_ = binary.Write(&frame, binary.BigEndian, uint32(len(payload)))
	_ = binary.Write(&frame, binary.BigEndian, crc32.ChecksumIEEE(payload))
	frame.Write(payload)
	if len(m.Segments) == 0 || filepath.Ext(m.Segments[len(m.Segments)-1].Name) == ".gz" || m.Segments[len(m.Segments)-1].Size+int64(frame.Len()) > segmentBytes {
		if len(m.Segments) > 0 && s.cfg.Compress {
			if err := s.seal(m); err != nil {
				return err
			}
		}
		name := fmt.Sprintf("%020d.records", m.Next)
		if m.HasPriorAttempts {
			name = m.Archive + "-" + name
		}
		m.Segments = append(m.Segments, segment{Name: name, First: m.Next, Next: m.Next})
	}
	part := &m.Segments[len(m.Segments)-1]
	f, e := os.OpenFile(filepath.Join(s.root, key(m.ID), part.Name), os.O_CREATE|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	_, e = f.Seek(part.Size, io.SeekStart)
	if e == nil {
		_, e = f.Write(frame.Bytes())
	}
	if e == nil {
		e = f.Truncate(part.Size + int64(frame.Len()))
	}
	if e == nil {
		e = f.Sync()
	}
	e = errors.Join(e, f.Close())
	if e != nil {
		return e
	}
	part.Size += int64(frame.Len())
	part.Next++
	m.Next++
	m.Bytes += int64(len(data))
	m.Updated = time.Now().UTC()
	m.History.Capture = "capturing"
	if !nativeTime.IsZero() {
		m.NativeTimestamp = nativeTime
		m.NativeSequence = r.Sequence
		m.NativeFingerprint = key(string(r.Data))
	}
	return nil
}
func (s *Store) fail(id string, err error) error {
	s.failed[id] = ErrStorage
	return fmt.Errorf("%w: %w", ErrStorage, err)
}
func (s *Store) seal(m *metadata) error {
	part := &m.Segments[len(m.Segments)-1]
	if filepath.Ext(part.Name) == ".gz" {
		return nil
	}
	dir := filepath.Join(s.root, key(m.ID))
	f, err := os.Open(filepath.Join(dir, part.Name))
	if err != nil {
		return err
	}
	defer f.Close()
	var b bytes.Buffer
	z := gzip.NewWriter(&b)
	_, err = io.Copy(z, io.LimitReader(f, part.Size))
	err = errors.Join(err, z.Close())
	if err != nil {
		return err
	}
	if err = atomic(dir, part.Name+".gz", b.Bytes()); err != nil {
		return err
	}
	old := part.Name
	part.Name += ".gz"
	part.Size = int64(b.Len())
	if err = s.save(*m); err != nil {
		return err
	}
	return os.Remove(filepath.Join(dir, old))
}
func (s *Store) Finish(id string, final, incomplete bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, err := load(filepath.Join(s.root, key(id)))
	if err != nil {
		return err
	}
	if m.Finalized {
		return nil
	}
	m.Finalized = final
	m.Incomplete = m.Incomplete || incomplete
	m.History.Incomplete = m.Incomplete
	m.Updated = time.Now().UTC()
	m.History.Capture = "interrupted"
	if final {
		m.ClosedAt = m.Updated
		m.History.Capture = "closed"
	}
	if _, bad := s.failed[id]; bad {
		m.Incomplete = true
		m.History.Incomplete = true
	}
	if err = s.save(m); err != nil {
		return err
	}
	if final && len(m.Segments) > 0 && s.cfg.Compress {
		return s.seal(&m)
	}
	return nil
}
func (s *Store) Histories() ([]History, error) { return Histories(s.root) }
func Histories(root string) ([]History, error) {
	entries, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return []History{}, nil
	}
	if err != nil {
		return nil, err
	}
	out := []History{}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		m, err := load(filepath.Join(root, e.Name()))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if key(m.ID) != e.Name() {
			return nil, ErrStorage
		}
		out = append(out, m.History)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SubmittedAt.After(out[j].SubmittedAt) })
	return out, nil
}
func (s *Store) Clean() error { s.mu.Lock(); defer s.mu.Unlock(); return s.cleanLocked() }
func (s *Store) cleanLocked() error {
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return err
	}
	type capture struct {
		metadata
		file string
	}
	var all []capture
	var total int64
	collect := func(m metadata, file string) {
		if m.Deleted {
			return
		}
		all = append(all, capture{m, file})
		for _, part := range m.Segments {
			total += part.Size
		}
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(s.root, e.Name())
		m, err := load(dir)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		collect(m, "metadata.json")
		if !m.HasPriorAttempts {
			continue
		}
		files, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, file := range files {
			name := file.Name()
			if !attemptSnapshot.MatchString(name) || name == snapshotName(m.Archive) {
				continue
			}
			prior, err := loadMetadata(dir, name)
			if err != nil || prior.ID != m.ID || !prior.Finalized || snapshotName(prior.Archive) != name {
				return ErrStorage
			}
			collect(prior, name)
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ClosedAt.Before(all[j].ClosedAt) })
	for _, m := range all {
		if !m.Finalized {
			continue
		}
		if total <= s.cfg.MaxBytes && time.Since(m.ClosedAt) <= s.cfg.MaxAge {
			continue
		}
		m.Deleted = true
		m.Incomplete = true
		m.History.Incomplete = true
		m.History.Capture = "expired"
		gap(&m.metadata, "archive_expired")
		if err = s.saveMetadata(m.metadata, m.file); err != nil {
			return err
		}
		for _, p := range m.Segments {
			if err = os.Remove(filepath.Join(s.root, key(m.ID), p.Name)); err != nil && !os.IsNotExist(err) {
				return err
			}
			total -= p.Size
		}
	}
	if total > s.cfg.MaxBytes {
		return ErrBudget
	}
	return nil
}

type checkpoint struct {
	Version        int
	Scope, Archive string
	Next           uint64
}

func (s *Store) GetLaunchLog(ctx context.Context, q api.LaunchLogRequest) (api.LaunchLogResponse, error) {
	return Read(ctx, s.root, q)
}
func readOnce(ctx context.Context, root string, q api.LaunchLogRequest) (api.LaunchLogResponse, error) {
	var zero api.LaunchLogResponse
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if !validID.MatchString(q.LaunchID) || q.PageSize < 0 || q.PageSize > 100 {
		return zero, api.ErrLaunchLogInvalidRequest
	}
	if q.PageSize == 0 {
		q.PageSize = 100
	}
	dir := filepath.Join(root, key(q.LaunchID))
	m, err := load(dir)
	if os.IsNotExist(err) || err == nil && m.Deleted && (q.PageToken == "" || !m.HasPriorAttempts) {
		return zero, api.ErrLaunchLogNotFound
	}
	if err != nil {
		return zero, err
	}
	if m.ID != q.LaunchID {
		return zero, ErrStorage
	}
	scope := key(root)
	cp := checkpoint{1, scope, m.Archive, 1}
	if q.PageToken != "" {
		if len(q.PageToken) > 16384 {
			return zero, api.ErrLaunchLogCursor
		}
		b, e := base64.RawURLEncoding.DecodeString(q.PageToken)
		if e != nil || json.Unmarshal(b, &cp) != nil || cp.Version != 1 || cp.Scope != scope {
			return zero, api.ErrLaunchLogCursor
		}
		if cp.Archive != m.Archive {
			if !m.HasPriorAttempts || !validArchive.MatchString(cp.Archive) {
				return zero, api.ErrLaunchLogCursor
			}
			m, err = loadMetadata(dir, snapshotName(cp.Archive))
			if os.IsNotExist(err) {
				return zero, api.ErrLaunchLogCursor
			}
			if err != nil || m.ID != q.LaunchID || !m.Finalized || m.Archive != cp.Archive {
				return zero, ErrStorage
			}
		}
		if m.Deleted {
			return zero, api.ErrLaunchLogNotFound
		}
		if cp.Next < 1 || cp.Next > m.Next {
			return zero, api.ErrLaunchLogCursor
		}
	}
	out := api.LaunchLogResponse{Records: []api.LaunchLogRecord{}, Finalized: m.Finalized, Incomplete: m.Incomplete}
	size := m.Bytes
	out.EstimatedTotalSizeBytes = &size
	for _, part := range m.Segments {
		if cp.Next >= part.Next {
			continue
		}
		if err = ctx.Err(); err != nil {
			return zero, err
		}
		f, e := os.Open(filepath.Join(dir, part.Name))
		if e != nil {
			return zero, fmt.Errorf("%w: %w", errChanged, e)
		}
		var reader io.Reader = io.LimitReader(f, part.Size)
		var z *gzip.Reader
		if filepath.Ext(part.Name) == ".gz" {
			z, e = gzip.NewReader(reader)
			if e != nil {
				f.Close()
				return zero, ErrStorage
			}
			reader = z
		}
		expected := part.First
		for expected < part.Next {
			record, frameErr := readFrame(reader, expected)
			if frameErr != nil {
				e = frameErr
				break
			}
			expected++
			if record.Sequence < cp.Next {
				continue
			}
			out.Records = append(out.Records, record)
			cp.Next = record.Sequence + 1
			if len(out.Records) == q.PageSize {
				break
			}
		}
		if z != nil {
			_ = z.Close()
		}
		f.Close()
		if e != nil {
			return zero, ErrStorage
		}
		if len(out.Records) == q.PageSize {
			break
		}
	}
	if cp.Next < m.Next && len(out.Records) == 0 {
		return zero, ErrStorage
	}
	out.HasMore = cp.Next < m.Next
	b, _ := json.Marshal(cp)
	out.NextPageToken = base64.RawURLEncoding.EncodeToString(b)
	return out, nil
}

func readFrame(reader io.Reader, sequence uint64) (api.LaunchLogRecord, error) {
	var record api.LaunchLogRecord
	var header [8]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return record, ErrStorage
	}
	size := binary.BigEndian.Uint32(header[:4])
	if size == 0 || size > 16384 {
		return record, ErrStorage
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(reader, payload); err != nil || crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(header[4:]) {
		return record, ErrStorage
	}
	if json.Unmarshal(payload, &record) != nil || record.Sequence != sequence || len(record.Data) > api.MaxLogRecordBytes {
		return api.LaunchLogRecord{}, ErrStorage
	}
	return record, nil
}

var errChanged = errors.New("archive snapshot changed")

func Read(ctx context.Context, root string, q api.LaunchLogRequest) (api.LaunchLogResponse, error) {
	for range 3 {
		p, err := readOnce(ctx, root, q)
		if !errors.Is(err, errChanged) {
			return p, err
		}
	}
	return api.LaunchLogResponse{}, ErrStorage
}
