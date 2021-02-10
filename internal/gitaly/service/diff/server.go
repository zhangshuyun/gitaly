package diff

import (
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

const msgSizeThreshold = 5 * 1024

type server struct {
	MsgSizeThreshold int
	cfg              config.Cfg
	locator          storage.Locator
	gitCmdFactory    git.CommandFactory
}

// NewServer creates a new instance of a gRPC DiffServer
func NewServer(cfg config.Cfg, locator storage.Locator, gitCmdFactory git.CommandFactory) gitalypb.DiffServiceServer {
	return &server{
		MsgSizeThreshold: msgSizeThreshold,
		cfg:              cfg,
		locator:          locator,
		gitCmdFactory:    gitCmdFactory,
	}
}
