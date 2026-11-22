package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// Docker Official Image: scratch plus the /hello executable. The classic image
// store cannot associate one index digest with two different platform images.
// Pin each candidate's child manifest so native and emulated probes can coexist.
const ProbeImageIndex = "docker.io/library/hello-world@sha256:5e22040d441e5fb3aed38368acbe8486b575d7018df38dbdfbc7311fbb2ef3a9"
const ProbeCommand = "/hello"

func ProbeImageForPlatform(platform string) string {
	switch platform {
	case "linux/arm64":
		return "docker.io/library/hello-world@sha256:5099b89d7666cc2186cad769ddc262ddc7c335b33f5fe79f9ffe50a01282b23e"
	case "linux/amd64":
		return "docker.io/library/hello-world@sha256:d1a8d0a4eeb63aff09f5f34d4d80505e0ba81905f36158cc3970d8e07179e59e"
	default:
		// Other native architectures use the index; neither candidate uses it.
		return ProbeImageIndex
	}
}

var ErrImagePlatform = errors.New("image platform differs")

type ImageInfo struct {
	ID           string `json:"Id"`
	RepoDigests  []string
	OS           string `json:"Os"`
	Architecture string
	Variant      string
	Config       struct {
		User string
		Env  []string
	}
}

func (e *Engine) PlatformInspection() bool {
	minor, err := strconv.Atoi(strings.TrimPrefix(e.Version, "/v1."))
	return err == nil && minor >= 49
}

func (i ImageInfo) Matches(platform string) bool {
	parts := strings.Split(platform, "/")
	return len(parts) >= 2 && i.OS == parts[0] && i.Architecture == parts[1] &&
		(len(parts) == 2 || i.Variant == parts[2])
}

// ResolveImage never uses a tag's default variant as a substitute for the
// requested platform. Its immutable ID is used for every subsequent create.
func (e *Engine) ResolveImage(ctx context.Context, ref, platform string) (ImageInfo, error) {

	image, err := e.InspectImage(ctx, ref, platform)
	// Classic stores may return the other variant instead of a 404. Pull that
	// platform explicitly, then inspect again and verify before any execution.
	if apiStatus(err, 404) || (err == nil && !image.Matches(platform)) {
		if err = e.Pull(ctx, ref, platform); err != nil {
			return ImageInfo{}, err
		}
		image, err = e.InspectImage(ctx, ref, platform)
	}
	if err != nil {
		return ImageInfo{}, err
	}
	if image.ID == "" {
		return ImageInfo{}, fmt.Errorf("Docker image has no ID")
	}
	if !image.Matches(platform) {
		return ImageInfo{}, ErrImagePlatform
	}
	return image, nil
}

func ContainerCreatePath(name, platform string) string {
	return "/containers/create?" + url.Values{"name": {name}, "platform": {platform}}.Encode()
}

func apiStatus(err error, code int) bool {
	var api *APIError
	return errors.As(err, &api) && api.Code == code
}

// InspectImage selects an immutable local variant without pulling.
func (e *Engine) InspectImage(ctx context.Context, ref, platform string) (ImageInfo, error) {
	path := "/images/" + url.PathEscape(ref) + "/json"
	query := ""
	if e.PlatformInspection() {
		parts := strings.SplitN(platform, "/", 3)
		if len(parts) < 2 {
			return ImageInfo{}, ErrImagePlatform
		}
		value := map[string]string{"os": parts[0], "architecture": parts[1]}
		if len(parts) == 3 {
			value["variant"] = parts[2]
		}
		encoded, _ := json.Marshal(value)
		query = "?" + url.Values{"platform": {string(encoded)}}.Encode()
	}

	var root ImageInfo
	if err := e.Call(ctx, "GET", path, nil, &root); err != nil {
		return ImageInfo{}, err
	}
	if root.ID == "" {
		return ImageInfo{}, fmt.Errorf("Docker image has no ID")
	}
	if query == "" {
		return root, nil
	}
	var selected ImageInfo
	// Containerd stores can return a child manifest ID from platform
	// inspection that is not independently addressable by create. Select
	// from the immutable, locally addressable root ID and create with that
	// root plus the explicit platform. Never re-read a mutable tag here.
	if err := e.Call(ctx, "GET", "/images/"+url.PathEscape(root.ID)+"/json"+query, nil, &selected); err != nil {
		return ImageInfo{}, err
	}
	if selected.ID == "" {
		return ImageInfo{}, fmt.Errorf("Docker platform image has no ID")
	}
	selected.ID = root.ID
	selected.RepoDigests = root.RepoDigests
	return selected, nil
}
