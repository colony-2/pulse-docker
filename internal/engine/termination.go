package engine

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"time"
)

var errObservation = errors.New("invalid Docker process observation")

type State struct {
	Status, StartedAt, FinishedAt, Error string
	ExitCode                             *int
	OOMKilled                            *bool
}

func DockerTime(raw string) *time.Time {
	stamp, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil || stamp.IsZero() || stamp.Year() <= 1 {
		return nil
	}
	utc := stamp.UTC()
	return &utc
}

func (s State) Stopped() bool { return s.Status == "exited" || s.Status == "dead" }

func (s State) Terminal() bool {
	started := DockerTime(s.StartedAt)
	if !s.Stopped() || started == nil || s.Error != "" || s.ExitCode == nil || *s.ExitCode < 0 {
		return false
	}
	if s.FinishedAt == "" || s.FinishedAt == "0001-01-01T00:00:00Z" {
		return true
	}
	finished := DockerTime(s.FinishedAt)
	return finished != nil && !finished.Before(*started)
}

type WorkerInspect struct {
	ID         string `json:"Id"`
	State      State
	Config     struct{ Labels map[string]string }
	HostConfig struct{ AutoRemove *bool }
}

func (e *Engine) InspectWorker(ctx context.Context, id string) (WorkerInspect, error) {
	var result WorkerInspect
	err := e.observation(ctx, http.MethodGet, "/containers/"+url.PathEscape(id)+"/json", 32<<20, &result)
	if err == nil && result.ID != id {
		err = errObservation
	}
	return result, err
}

func (e *Engine) WaitWorker(ctx context.Context, id string) (int, error) {
	var result struct {
		StatusCode *int
		Error      *struct{ Message string }
	}
	if err := e.observation(ctx, http.MethodPost, "/containers/"+url.PathEscape(id)+"/wait?condition=not-running", 64<<10, &result); err != nil {
		return 0, err
	}
	if result.StatusCode == nil || *result.StatusCode < 0 || result.Error != nil {
		return 0, errObservation
	}
	return *result.StatusCode, nil
}

func (e *Engine) observation(ctx context.Context, method, path string, limit int64, result any) error {
	req, err := http.NewRequestWithContext(ctx, method, e.Base+e.Version+path, nil)
	if err != nil {
		return errObservation
	}
	resp, err := e.Client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errObservation
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return &APIError{Code: resp.StatusCode}
	}
	body := &io.LimitedReader{R: resp.Body, N: limit + 1}
	decoder := json.NewDecoder(body)
	if decoder.Decode(result) != nil || decoder.Decode(new(json.RawMessage)) != io.EOF || body.N == 0 {
		return errObservation
	}
	return nil
}
