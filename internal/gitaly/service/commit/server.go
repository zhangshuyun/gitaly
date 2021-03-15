package commit

import (
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/linguist"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

type server struct {
	cfg           config.Cfg
	locator       storage.Locator
	gitCmdFactory git.CommandFactory
	linguist      *linguist.Instance
}

var (
	defaultBranchName = ref.DefaultBranchName
)

// NewServer creates a new instance of a grpc CommitServiceServer
func NewServer(cfg config.Cfg, locator storage.Locator, gitCmdFactory git.CommandFactory, ling *linguist.Instance) gitalypb.CommitServiceServer {
	return &server{cfg: cfg, locator: locator, gitCmdFactory: gitCmdFactory, linguist: ling}
}
