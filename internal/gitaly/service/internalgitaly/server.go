package internalgitaly

import (
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

type server struct {
	gitalypb.UnimplementedInternalGitalyServer
	storages []config.Storage
}

// NewServer return an instance of the Gitaly service.
func NewServer(storages []config.Storage) gitalypb.InternalGitalyServer {
	return &server{storages: storages}
}
