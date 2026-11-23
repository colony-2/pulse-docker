package provider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"hash"
	"time"
	"unicode/utf8"

	protocol "github.com/colony-2/pulse-docker/internal/engine"
	api "github.com/colony-2/pulse-provider-api"
)

type jsonFileDigest struct {
	hash.Hash
	pending []byte
}

func (digest *jsonFileDigest) write(data []byte) {
	if len(digest.pending) != 0 {
		data = append(digest.pending, data...)
		digest.pending = nil
	}
	for len(data) != 0 {
		count := 0
		for count < len(data) && utf8.FullRune(data[count:]) {
			runeValue, size := utf8.DecodeRune(data[count:])
			if runeValue == utf8.RuneError && size == 1 {
				break
			}
			count += size
		}
		digest.Write(data[:count])
		data = data[count:]
		if len(data) == 0 {
			return
		}
		if !utf8.FullRune(data) {
			digest.pending = bytes.Clone(data)
			return
		}
		digest.Write([]byte("\uFFFD"))
		data = data[1:]
	}
}

func (digest *jsonFileDigest) finish() []byte {
	for range digest.pending {
		digest.Write([]byte("\uFFFD"))
	}
	return digest.Sum(nil)
}

func (c *captureGroup) equivalentRetainedOutput(engine *engine, id, container string) bool {
	ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
	defer cancel()
	archive := map[api.LogSource]*jsonFileDigest{
		api.LogSourceStdout: {Hash: sha256.New()},
		api.LogSourceStderr: {Hash: sha256.New()},
	}
	request := api.LaunchLogRequest{LaunchID: id}
	for {
		page, err := c.store.GetLaunchLog(ctx, request)
		if err != nil {
			return false
		}
		for _, record := range page.Records {
			if stream := archive[record.Source]; stream != nil {
				stream.write(record.Data)
			}
		}
		if !page.HasMore {
			break
		}
		request.PageToken = page.NextPageToken
	}
	retained := map[api.LogSource]hash.Hash{api.LogSourceStdout: sha256.New(), api.LogSourceStderr: sha256.New()}
	output, err := engine.ReadLogs(ctx, container, false)
	if err != nil {
		return false
	}
	defer output.Close()
	if err := protocol.ReadTimestampedOutput(output, func(source api.LogSource, _ time.Time, data []byte) error {
		_, err := retained[source].Write(data)
		return err
	}); err != nil {
		return false
	}
	for source, digest := range archive {
		if !bytes.Equal(digest.finish(), retained[source].Sum(nil)) {
			return false
		}
	}
	return true
}
