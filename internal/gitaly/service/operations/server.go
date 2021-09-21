package operations

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/quarantine"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

type Server struct {
	gitalypb.UnimplementedOperationServiceServer
	cfg           config.Cfg
	ruby          *rubyserver.Server
	hookManager   hook.Manager
	txManager     transaction.Manager
	locator       storage.Locator
	conns         *client.Pool
	git2go        git2go.Executor
	gitCmdFactory git.CommandFactory
	catfileCache  catfile.Cache
	updater       *updateref.UpdaterWithHooks

	// enableUserMergeBranchStructuredErrors enables the use of structured errors in
	// UserMergeBranch. This flag only exists for testing purposes.
	enableUserMergeBranchStructuredErrors bool
}

// NewServer creates a new instance of a grpc OperationServiceServer
func NewServer(
	cfg config.Cfg,
	rs *rubyserver.Server,
	hookManager hook.Manager,
	txManager transaction.Manager,
	locator storage.Locator,
	conns *client.Pool,
	gitCmdFactory git.CommandFactory,
	catfileCache catfile.Cache,
) *Server {
	return &Server{
		ruby:          rs,
		cfg:           cfg,
		hookManager:   hookManager,
		txManager:     txManager,
		locator:       locator,
		conns:         conns,
		git2go:        git2go.NewExecutor(cfg, locator),
		gitCmdFactory: gitCmdFactory,
		catfileCache:  catfileCache,
		updater:       updateref.NewUpdaterWithHooks(cfg, hookManager, gitCmdFactory, catfileCache),
	}
}

func (s *Server) localrepo(repo repository.GitRepo) *localrepo.Repo {
	return localrepo.New(s.gitCmdFactory, s.catfileCache, repo, s.cfg)
}

func (s *Server) quarantinedRepo(
	ctx context.Context, repo *gitalypb.Repository,
) (*quarantine.Dir, *localrepo.Repo, error) {
	quarantineDir, err := quarantine.New(ctx, repo, s.locator)
	if err != nil {
		return nil, nil, helper.ErrInternalf("creating object quarantine: %w", err)
	}

	quarantineRepo := s.localrepo(quarantineDir.QuarantinedRepo())

	return quarantineDir, quarantineRepo, nil
}
