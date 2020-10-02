package objectpool

import (
	"gitlab.com/gitlab-org/gitaly/v13/internal/storage"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
)

type server struct {
	locator storage.Locator
}

// NewServer creates a new instance of a gRPC repo server
func NewServer(locator storage.Locator) gitalypb.ObjectPoolServiceServer {
	return &server{locator: locator}
}
