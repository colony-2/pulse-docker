package docker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
)

type engine struct {
	client        *http.Client
	base, version string
	registryAuth  string
}
type apiError struct {
	code    int
	message string // Only synthetic startup probes may expose daemon details.
}

type pullError struct {
	message string // Retained only for synthetic startup probe diagnostics.
}

func (e *pullError) Error() string    { return "Docker image pull failed" }
func (e *pullError) GoString() string { return e.Error() }

func (e *apiError) Error() string    { return fmt.Sprintf("Docker API returned HTTP %d", e.code) }
func (e *apiError) GoString() string { return e.Error() }

func (e *engine) readAPIError(resp *http.Response) error {
	var body struct{ Message string }
	_ = json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&body)
	return &apiError{code: resp.StatusCode, message: e.redactMessage(body.Message)}
}

// Synthetic startup probes retain daemon detail, but a registry response may
// echo its authorization header. Strip the known credential before retaining it.
func (e *engine) redactMessage(message string) string {
	if e.registryAuth != "" {
		return strings.ReplaceAll(message, e.registryAuth, "[REDACTED]")
	}
	return message
}

// Requests use their caller's deadline, including slow image pulls and
// container creation. A shorter transport deadline would
// interrupt provisioning even when the configured call budget permits it.
func newEngine(socket string) *engine {
	return &engine{base: "http://docker", client: &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", strings.TrimPrefix(socket, "unix://"))
	}}}}
}
func (e *engine) call(ctx context.Context, method, path string, body any, out any) error {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, e.base+e.version+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if strings.HasPrefix(path, "/images/create") && e.registryAuth != "" {
		req.Header.Set("X-Registry-Auth", e.registryAuth)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return e.readAPIError(resp)
	}
	if out == nil {
		_, err = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		return err
	}
	dec := json.NewDecoder(io.LimitReader(resp.Body, 32<<20))
	return dec.Decode(out)
}
func (e *engine) pull(ctx context.Context, image, platform string) error {
	q := url.Values{"fromImage": {image}, "platform": {platform}}
	req, err := http.NewRequestWithContext(ctx, "POST", e.base+e.version+"/images/create?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	if e.registryAuth != "" {
		req.Header.Set("X-Registry-Auth", e.registryAuth)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return e.readAPIError(resp)
	}
	d := json.NewDecoder(io.LimitReader(resp.Body, 64<<20))
	for {
		var item struct {
			Error       string                    `json:"error"`
			ErrorDetail *struct{ Message string } `json:"errorDetail"`
		}
		err = d.Decode(&item)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		message := item.Error
		if item.ErrorDetail != nil && item.ErrorDetail.Message != "" {
			message = item.ErrorDetail.Message
		}
		if message != "" {
			return &pullError{message: e.redactMessage(message)}
		}
	}
}
