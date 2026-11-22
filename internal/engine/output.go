package engine

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	api "github.com/colony-2/pulse-provider-api"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"
)

type output struct {
	net.Conn
	reader *bufio.Reader
	stop   func() bool
}

func (o *output) Read(b []byte) (int, error) { return o.reader.Read(b) }
func (o *output) Close() error               { o.stop(); return o.Conn.Close() }

// AttachOutput preserves bytes buffered beyond the upgrade headers. Stdin is
// never selected: lease delivery owns its own independent attachment.
func (e *Engine) AttachOutput(ctx context.Context, id string) (io.ReadCloser, error) {
	u, err := url.Parse(e.Base)
	if err != nil {
		return nil, err
	}
	var conn net.Conn
	if tr, ok := e.Client.Transport.(*http.Transport); ok && tr.DialContext != nil {
		conn, err = tr.DialContext(ctx, "tcp", u.Host)
	} else {
		conn, err = (&net.Dialer{}).DialContext(ctx, "tcp", u.Host)
	}
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	fail := func(err error) (io.ReadCloser, error) { stop(); conn.Close(); return nil, err }
	req, err := http.NewRequest("POST", e.Base+e.Version+"/containers/"+url.PathEscape(id)+"/attach?stream=1&stdin=0&stdout=1&stderr=1", nil)
	if err != nil {
		return fail(err)
	}
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "tcp")
	if err = req.Write(conn); err != nil {
		return fail(err)
	}
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, req)
	if err != nil {
		return fail(err)
	}
	if resp.StatusCode != 101 {
		defer resp.Body.Close()
		return fail(e.ReadAPIError(resp))
	}
	return &output{conn, reader, stop}, nil
}

// ReadOutput validates Docker's non-TTY framing without allocating by frame size.
func ReadOutput(r io.Reader, emit func(api.LogSource, []byte) error) error {
	var header [8]byte
	buffer := make([]byte, api.MaxLogRecordBytes)
	for {
		_, err := io.ReadFull(r, header[:])
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if (header[0] != 1 && header[0] != 2) || header[1] != 0 || header[2] != 0 || header[3] != 0 {
			return fmt.Errorf("invalid Docker output frame")
		}
		n := binary.BigEndian.Uint32(header[4:])
		if n > 16<<20 {
			return fmt.Errorf("oversized Docker output frame")
		}
		source := api.LogSourceStdout
		if header[0] == 2 {
			source = api.LogSourceStderr
		}
		for n > 0 {
			size := min(int(n), len(buffer))
			if _, err = io.ReadFull(r, buffer[:size]); err != nil {
				return err
			}
			if err = emit(source, buffer[:size]); err != nil {
				return err
			}
			n -= uint32(size)
		}
	}
}

// Logs replays whatever Docker still retains. Recovery must disclose uncertainty
// because timestamps/rotation cannot establish an exactly-once archive boundary.
func (e *Engine) Logs(ctx context.Context, id string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", e.Base+e.Version+"/containers/"+url.PathEscape(id)+"/logs?follow=1&timestamps=1&stdout=1&stderr=1", nil)
	if err != nil {
		return nil, err
	}
	resp, err := e.Client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		defer resp.Body.Close()
		return nil, e.ReadAPIError(resp)
	}
	return resp.Body, nil
}

// ReadTimestampedOutput reads Docker's retained log entries. Each native entry
// carries its daemon timestamp; that prefix is metadata, never worker bytes.
func ReadTimestampedOutput(r io.Reader, emit func(api.LogSource, time.Time, []byte) error) error {
	var header [8]byte
	buffer := make([]byte, api.MaxLogRecordBytes)
	for {
		_, err := io.ReadFull(r, header[:])
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if (header[0] != 1 && header[0] != 2) || header[1] != 0 || header[2] != 0 || header[3] != 0 {
			return fmt.Errorf("invalid Docker output frame")
		}
		n := binary.BigEndian.Uint32(header[4:])
		if n == 0 || n > 16<<20 {
			return fmt.Errorf("invalid Docker retained frame size")
		}
		var stamp [64]byte
		used := 0
		for {
			if n == 0 || used == len(stamp) {
				return fmt.Errorf("invalid Docker log timestamp")
			}
			if _, err = io.ReadFull(r, stamp[used:used+1]); err != nil {
				return err
			}
			n--
			if stamp[used] == ' ' {
				break
			}
			used++
		}
		native, err := time.Parse(time.RFC3339Nano, string(stamp[:used]))
		if err != nil {
			return fmt.Errorf("invalid Docker log timestamp")
		}
		source := api.LogSourceStdout
		if header[0] == 2 {
			source = api.LogSourceStderr
		}
		for n > 0 {
			size := min(int(n), len(buffer))
			if _, err = io.ReadFull(r, buffer[:size]); err != nil {
				return err
			}
			if err = emit(source, native, buffer[:size]); err != nil {
				return err
			}
			n -= uint32(size)
		}
	}
}
