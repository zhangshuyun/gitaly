package namespace

import (
	"gitlab.com/gitlab-org/gitaly/v14/internal/storage"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

type server struct {
	locator storage.Locator
}

// NewServer creates a new instance of a gRPC namespace server
func NewServer(locator storage.Locator) gitalypb.NamespaceServiceServer {
	return &server{locator: locator}
}
