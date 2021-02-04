package commit

import (
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

type server struct {
	locator       storage.Locator
	cfg           config.Cfg
	gitCmdFactory git.CommandFactory
}

var (
	defaultBranchName = ref.DefaultBranchName
)

// NewServer creates a new instance of a grpc CommitServiceServer
func NewServer(cfg config.Cfg, locator storage.Locator, gitCmdFactory git.CommandFactory) gitalypb.CommitServiceServer {
	return &server{cfg: cfg, locator: locator, gitCmdFactory: gitCmdFactory}
}
