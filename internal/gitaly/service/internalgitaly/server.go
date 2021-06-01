package internalgitaly

import (
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

type server struct {
	storages []config.Storage
}

func NewServer(storages []config.Storage) gitalypb.InternalGitalyServer {
	return &server{storages: storages}
}
