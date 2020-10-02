package diff

import (
	"gitlab.com/gitlab-org/gitaly/v13/internal/storage"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
)

const msgSizeThreshold = 5 * 1024

type server struct {
	MsgSizeThreshold int
	locator          storage.Locator
}

// NewServer creates a new instance of a gRPC DiffServer
func NewServer(locator storage.Locator) gitalypb.DiffServiceServer {
	return &server{
		MsgSizeThreshold: msgSizeThreshold,
		locator:          locator,
	}
}
