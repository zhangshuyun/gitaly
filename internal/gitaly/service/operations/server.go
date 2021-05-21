package operations

import (
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
)

type Server struct {
	cfg           config.Cfg
	ruby          *rubyserver.Server
	hookManager   hook.Manager
	locator       storage.Locator
	conns         *client.Pool
	git2go        git2go.Executor
	gitCmdFactory git.CommandFactory
	catfileCache  catfile.Cache
}

// NewServer creates a new instance of a grpc OperationServiceServer
func NewServer(
	cfg config.Cfg,
	rs *rubyserver.Server,
	hookManager hook.Manager,
	locator storage.Locator,
	conns *client.Pool,
	gitCmdFactory git.CommandFactory,
	catfileCache catfile.Cache,
) *Server {
	return &Server{
		ruby:          rs,
		cfg:           cfg,
		hookManager:   hookManager,
		locator:       locator,
		conns:         conns,
		git2go:        git2go.New(filepath.Join(cfg.BinDir, "gitaly-git2go"), cfg.Git.BinPath),
		gitCmdFactory: gitCmdFactory,
		catfileCache:  catfileCache,
	}
}

func (s *Server) localrepo(repo repository.GitRepo) *localrepo.Repo {
	return localrepo.New(s.gitCmdFactory, s.catfileCache, repo, s.cfg)
}
