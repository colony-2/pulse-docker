package engine

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"

	compute "github.com/colony-2/pulse-docker/internal/native"
)

// Start attaches before starting so an executor reading its lease immediately
// cannot race the attachment. StdinOnce closes stdin when this connection ends.
// The capability never enters container environment variables or arguments.
func (e *Engine) Start(ctx context.Context, id string, input compute.SecretInput) error {
	path := "/containers/" + url.PathEscape(id)
	if input == "" {
		return e.Call(ctx, "POST", path+"/start", nil, nil)
	}
	u, err := url.Parse(e.Base)
	if err != nil {
		return err
	}
	var conn net.Conn
	if tr, ok := e.Client.Transport.(*http.Transport); ok && tr.DialContext != nil {
		conn, err = tr.DialContext(ctx, "tcp", u.Host)
	} else {
		conn, err = (&net.Dialer{}).DialContext(ctx, "tcp", u.Host)
	}
	if err != nil {
		return err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	if deadline, ok := ctx.Deadline(); ok {
		conn.SetDeadline(deadline)
	}
	req, err := http.NewRequest("POST", e.Base+e.Version+path+"/attach?stream=1&stdin=1&stdout=0&stderr=0", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "tcp")
	if err := req.Write(conn); err != nil {
		return err
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		defer resp.Body.Close()
		return fmt.Errorf("Docker stdin attachment: %w", e.ReadAPIError(resp))
	}
	if err := e.Call(ctx, "POST", path+"/start", nil, nil); err != nil {
		return err
	}
	_, err = io.Copy(conn, strings.NewReader(string(input)))
	return err
}
