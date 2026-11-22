package provider

import protocol "github.com/colony-2/pulse-docker/internal/engine"

type engine = protocol.Engine
type apiError = protocol.APIError
type pullError = protocol.PullError
type imageInfo = protocol.ImageInfo

var newEngine = protocol.New
var ResolveSocket = protocol.ResolveSocket
var containerCreatePath = protocol.ContainerCreatePath
var probeImageForPlatform = protocol.ProbeImageForPlatform
var errImagePlatform = protocol.ErrImagePlatform

const probeCommand = protocol.ProbeCommand
const probeImageIndex = protocol.ProbeImageIndex
