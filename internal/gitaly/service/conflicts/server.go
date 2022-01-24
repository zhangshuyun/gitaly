package conflicts

import (
	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

type server struct {
	gitalypb.UnimplementedConflictsServiceServer
	locator        storage.Locator
	gitCmdFactory  git.CommandFactory
	catfileCache   catfile.Cache
	pool           *client.Pool
	hookManager    hook.Manager
	updater        *updateref.UpdaterWithHooks
	git2goExecutor *git2go.Executor
}

// NewServer creates a new instance of a grpc ConflictsServer
func NewServer(
	hookManager hook.Manager,
	locator storage.Locator,
	gitCmdFactory git.CommandFactory,
	catfileCache catfile.Cache,
	connsPool *client.Pool,
	git2goExecutor *git2go.Executor,
	updater *updateref.UpdaterWithHooks,
) gitalypb.ConflictsServiceServer {
	return &server{
		hookManager:    hookManager,
		locator:        locator,
		gitCmdFactory:  gitCmdFactory,
		catfileCache:   catfileCache,
		pool:           connsPool,
		updater:        updater,
		git2goExecutor: git2goExecutor,
	}
}

func (s *server) localrepo(repo repository.GitRepo) *localrepo.Repo {
	return localrepo.New(s.locator, s.gitCmdFactory, s.catfileCache, repo)
}
