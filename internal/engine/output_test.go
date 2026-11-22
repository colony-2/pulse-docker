package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	api "github.com/colony-2/pulse-provider-api"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func outputFrame(src byte, b []byte) []byte {
	h := make([]byte, 8)
	h[0] = src
	binary.BigEndian.PutUint32(h[4:], uint32(len(b)))
	return append(h, b...)
}
func TestOutputBinaryFragmentationAndFraming(t *testing.T) {
	raw := bytes.Repeat([]byte{0, 255, 128}, 4000)
	stream := append(outputFrame(1, raw), outputFrame(2, []byte("err"))...)
	var got []byte
	count := 0
	err := ReadOutput(bytes.NewReader(stream), func(src api.LogSource, b []byte) error {
		if len(b) > 4096 {
			t.Fatal("unbounded record")
		}
		count++
		if src == api.LogSourceStdout {
			got = append(got, b...)
		} else if src != api.LogSourceStderr {
			t.Fatal(src)
		}
		return nil
	})
	if err != nil || !bytes.Equal(raw, got) || count != 4 {
		t.Fatal(count, err)
	}
	for _, b := range [][]byte{{1}, outputFrame(0, []byte("a")), {1, 0, 1, 0, 0, 0, 0, 0}, {1, 0, 0, 0, 255, 255, 255, 255}, outputFrame(1, []byte("x"))[:8]} {
		if ReadOutput(bytes.NewReader(b), func(api.LogSource, []byte) error { return nil }) == nil {
			t.Fatal("bad frame accepted", b)
		}
	}
}
func TestOutputAttachmentRetainsBufferedBytes(t *testing.T) {
	payload := outputFrame(2, []byte{0, 255, 1})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("stdin") != "0" || r.URL.Query().Get("stdout") != "1" || r.URL.Query().Get("stderr") != "1" {
			t.Error(r.URL)
		}
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n\r\n")
		rw.Write(payload)
		rw.Flush()
	}))
	defer server.Close()
	e := &Engine{Base: server.URL, Client: server.Client()}
	r, err := e.AttachOutput(t.Context(), "one")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(b, payload) {
		t.Fatal(b, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err = e.AttachOutput(ctx, "one"); err == nil {
		t.Fatal("cancellation ignored")
	}
}
func TestOutputRetainedNativeTimestampAndBinaryBytes(t *testing.T) {
	stamp := time.Now().UTC().Truncate(time.Nanosecond)
	raw := []byte{0, 255, 10}
	frame := outputFrame(2, append([]byte(stamp.Format(time.RFC3339Nano)+" "), raw...))
	count := 0
	err := ReadTimestampedOutput(bytes.NewReader(frame), func(src api.LogSource, ts time.Time, b []byte) error {
		count++
		if src != api.LogSourceStderr || !ts.Equal(stamp) || !bytes.Equal(b, raw) {
			t.Error(src, ts, b)
		}
		return nil
	})
	if err != nil || count != 1 {
		t.Fatal(count, err)
	}
	if ReadTimestampedOutput(bytes.NewReader(outputFrame(1, []byte("missing timestamp"))), func(api.LogSource, time.Time, []byte) error { return nil }) == nil {
		t.Fatal("accepted missing native timestamp")
	}
}
