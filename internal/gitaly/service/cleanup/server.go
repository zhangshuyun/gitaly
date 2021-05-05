package cleanup

import (
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

type server struct {
	cfg           config.Cfg
	gitCmdFactory git.CommandFactory
	catfileCache  catfile.Cache
}

// NewServer creates a new instance of a grpc CleanupServer
func NewServer(cfg config.Cfg, gitCmdFactory git.CommandFactory, catfileCache catfile.Cache) gitalypb.CleanupServiceServer {
	return &server{
		cfg:           cfg,
		gitCmdFactory: gitCmdFactory,
		catfileCache:  catfileCache,
	}
}
