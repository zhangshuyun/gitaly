package ref

import (
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

type server struct {
	cfg           config.Cfg
	txManager     transaction.Manager
	locator       storage.Locator
	gitCmdFactory git.CommandFactory
}

// NewServer creates a new instance of a grpc RefServer
func NewServer(cfg config.Cfg, locator storage.Locator, gitCmdFactory git.CommandFactory, txManager transaction.Manager) gitalypb.RefServiceServer {
	return &server{
		cfg:           cfg,
		txManager:     txManager,
		locator:       locator,
		gitCmdFactory: gitCmdFactory,
	}
}
