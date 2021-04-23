package objectpool

import (
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

type server struct {
	gitalypb.UnimplementedObjectPoolServiceServer
	cfg           config.Cfg
	locator       storage.Locator
	gitCmdFactory git.CommandFactory
}

// NewServer creates a new instance of a gRPC repo server
func NewServer(cfg config.Cfg, locator storage.Locator, gitCmdFactory git.CommandFactory) gitalypb.ObjectPoolServiceServer {
	return &server{cfg: cfg, locator: locator, gitCmdFactory: gitCmdFactory}
}
