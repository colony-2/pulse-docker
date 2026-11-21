package docker

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	compute "github.com/colony-2/pulse-docker/internal/native"
)

func TestNativeStdinDeliveryAndEOF(t *testing.T) {
	attached := make(chan struct{})
	received := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/attach") {
			if r.URL.Query().Get("stdin") != "1" || r.URL.Query().Get("stdout") != "0" || r.Header.Get("Upgrade") != "tcp" {
				t.Error("incorrect attachment")
			}
			conn, rw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			// Publish attachment before the response lets the client start. The
			// /start handler can run as soon as Flush sends the upgrade response.
			close(attached)
			rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n\r\n")
			rw.Flush()
			data, err := io.ReadAll(rw)
			if err != nil {
				t.Error(err)
			}
			received <- string(data)
		} else if strings.HasSuffix(r.URL.Path, "/start") {
			select {
			case <-attached:
			default:
				t.Error("started before attaching")
			}
			w.WriteHeader(204)
		} else {
			t.Error(r.URL)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	e := &engine{base: server.URL, client: server.Client()}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	input := strings.Repeat("private capability\n", 16000)
	if err := e.start(ctx, "job", compute.SecretInput(input)); err != nil {
		t.Fatal(err)
	}
	select {
	case data := <-received:
		if data != input {
			t.Fatalf("stdin truncated: got %d bytes want %d", len(data), len(input))
		}
	case <-ctx.Done():
		t.Fatal("stdin did not reach EOF")
	}
}

func TestAttachmentFailureDoesNotStartOrExposeInput(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/start") {
			t.Error("started without stdin")
		}
		w.WriteHeader(500)
		io.WriteString(w, `{"message":"private-capability"}`)
	}))
	defer server.Close()
	e := &engine{base: server.URL, client: server.Client()}
	if err := e.start(context.Background(), "job", "private-capability"); err == nil || strings.Contains(err.Error(), "private-capability") {
		t.Fatal("unredacted attachment error", err)
	}
}

func TestBlockedAttachmentHonorsCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	e := &engine{base: server.URL, client: server.Client()}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := e.start(ctx, "job", "private-capability"); err == nil {
		t.Fatal("cancelled attachment succeeded")
	}
}

func TestBlockedLeaseWriteHonorsCancellation(t *testing.T) {
	var calls atomic.Int32
	attached := make(chan struct{})
	started := make(chan struct{})
	peers := make(chan net.Conn, 2)
	transport := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		client, server := net.Pipe()
		peers <- server
		call := calls.Add(1)
		go func() {
			reader := bufio.NewReader(server)
			req, err := http.ReadRequest(reader)
			if err != nil {
				return
			}
			req.Body.Close()
			if call == 1 {
				if !strings.Contains(req.URL.Path, "/attach") {
					t.Error("expected attachment before start", req.URL)
				}
				fmt.Fprint(server, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n\r\n")
				close(attached)
				// Deliberately never read the body. net.Pipe has no buffer, so sending the
				// lease cannot complete until cancellation closes the client connection.
			} else {
				<-attached
				if !strings.HasSuffix(req.URL.Path, "/start") {
					t.Error("expected start", req.URL)
				}
				fmt.Fprint(server, "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
				close(started)
			}
		}()
		return client, nil
	}}
	defer transport.CloseIdleConnections()
	defer func() {
		close(peers)
		for peer := range peers {
			peer.Close()
		}
	}()
	e := &engine{base: "http://docker", client: &http.Client{Transport: transport}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- e.start(ctx, "worker", compute.SecretInput(strings.Repeat("synthetic-secret", 65536))) }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("start did not follow attachment")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil || strings.Contains(err.Error(), "synthetic-secret") {
			t.Fatal("write did not fail safely", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancellation did not close blocked write")
	}
}
