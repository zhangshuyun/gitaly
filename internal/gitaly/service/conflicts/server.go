package conflicts

import (
	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

type server struct {
	gitalypb.UnimplementedConflictsServiceServer
	cfg           config.Cfg
	locator       storage.Locator
	gitCmdFactory git.CommandFactory
	catfileCache  catfile.Cache
	pool          *client.Pool
	hookManager   hook.Manager
	updater       *updateref.UpdaterWithHooks
	git2go        git2go.Executor
}

// NewServer creates a new instance of a grpc ConflictsServer
func NewServer(cfg config.Cfg, hookManager hook.Manager, locator storage.Locator, gitCmdFactory git.CommandFactory, catfileCache catfile.Cache) gitalypb.ConflictsServiceServer {
	return &server{
		cfg:           cfg,
		hookManager:   hookManager,
		locator:       locator,
		gitCmdFactory: gitCmdFactory,
		catfileCache:  catfileCache,
		pool: client.NewPoolWithOptions(
			client.WithDialer(client.HealthCheckDialer(client.DialContext)),
			client.WithDialOptions(client.FailOnNonTempDialError()...),
		),
		updater: updateref.NewUpdaterWithHooks(cfg, hookManager, gitCmdFactory, catfileCache),
		git2go:  git2go.NewExecutor(cfg, locator),
	}
}

func (s *server) localrepo(repo repository.GitRepo) *localrepo.Repo {
	return localrepo.New(s.gitCmdFactory, s.catfileCache, repo, s.cfg)
}
