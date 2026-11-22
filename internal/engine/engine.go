package engine

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

type Engine struct {
	Client        *http.Client
	Base, Version string
	RegistryAuth  string
}
type APIError struct {
	Code    int
	Message string // Only synthetic startup probes may expose daemon details.
}

type PullError struct {
	Message string // Retained only for synthetic startup probe diagnostics.
}

func (e *PullError) Error() string    { return "Docker image pull failed" }
func (e *PullError) GoString() string { return e.Error() }

func (e *APIError) Error() string    { return fmt.Sprintf("Docker API returned HTTP %d", e.Code) }
func (e *APIError) GoString() string { return e.Error() }

func (e *Engine) ReadAPIError(resp *http.Response) error {
	var body struct{ Message string }
	_ = json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&body)
	return &APIError{Code: resp.StatusCode, Message: e.RedactMessage(body.Message)}
}

// Synthetic startup probes retain daemon detail, but a registry response may
// echo its authorization header. Strip the known credential before retaining it.
func (e *Engine) RedactMessage(Message string) string {
	if e.RegistryAuth != "" {
		return strings.ReplaceAll(Message, e.RegistryAuth, "[REDACTED]")
	}
	return Message
}

// Requests use their caller's deadline, including slow image pulls and
// container creation. A shorter transport deadline would
// interrupt provisioning even when the configured Call budget permits it.
func New(socket string) *Engine {
	return &Engine{Base: "http://docker", Client: &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", strings.TrimPrefix(socket, "unix://"))
	}}}}
}
func (e *Engine) Call(ctx context.Context, method, path string, body any, out any) error {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, e.Base+e.Version+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if strings.HasPrefix(path, "/images/create") && e.RegistryAuth != "" {
		req.Header.Set("X-Registry-Auth", e.RegistryAuth)
	}
	resp, err := e.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return e.ReadAPIError(resp)
	}
	if out == nil {
		_, err = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		return err
	}
	dec := json.NewDecoder(io.LimitReader(resp.Body, 32<<20))
	return dec.Decode(out)
}
func (e *Engine) Pull(ctx context.Context, image, platform string) error {
	_, err := e.PullDigest(ctx, image, platform)
	return err
}
func (e *Engine) PullDigest(ctx context.Context, image, platform string) (string, error) {
	manifest := ""
	q := url.Values{"fromImage": {image}, "platform": {platform}}
	req, err := http.NewRequestWithContext(ctx, "POST", e.Base+e.Version+"/images/create?"+q.Encode(), nil)
	if err != nil {
		return "", err
	}
	if e.RegistryAuth != "" {
		req.Header.Set("X-Registry-Auth", e.RegistryAuth)
	}
	resp, err := e.Client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", e.ReadAPIError(resp)
	}
	d := json.NewDecoder(io.LimitReader(resp.Body, 64<<20))
	for {
		var item struct {
			Status      string                    `json:"status"`
			Error       string                    `json:"error"`
			ErrorDetail *struct{ Message string } `json:"errorDetail"`
		}
		err = d.Decode(&item)
		if err == io.EOF {
			return manifest, nil
		}
		if err != nil {
			return "", err
		}
		if strings.HasPrefix(item.Status, "Digest: ") {
			manifest = strings.TrimSpace(strings.TrimPrefix(item.Status, "Digest: "))
		}
		Message := item.Error
		if item.ErrorDetail != nil && item.ErrorDetail.Message != "" {
			Message = item.ErrorDetail.Message
		}
		if Message != "" {
			return "", &PullError{Message: e.RedactMessage(Message)}
		}
	}
}
