package diff

import (
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
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
	catfileCache     catfile.Cache
}

// NewServer creates a new instance of a gRPC DiffServer
func NewServer(cfg config.Cfg, locator storage.Locator, gitCmdFactory git.CommandFactory, catfileCache catfile.Cache) gitalypb.DiffServiceServer {
	return &server{
		MsgSizeThreshold: msgSizeThreshold,
		cfg:              cfg,
		locator:          locator,
		gitCmdFactory:    gitCmdFactory,
		catfileCache:     catfileCache,
	}
}

func (s *server) localrepo(repo repository.GitRepo) *localrepo.Repo {
	return localrepo.New(s.gitCmdFactory, s.catfileCache, repo, s.cfg)
}
