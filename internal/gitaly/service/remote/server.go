package remote

import (
	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/env"
	"gitlab.com/gitlab-org/gitaly/v14/internal/storage"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

type server struct {
	gitalypb.UnimplementedRemoteServiceServer
	cfg           config.Cfg
	ruby          *rubyserver.Server
	locator       storage.Locator
	gitCmdFactory git.CommandFactory
	catfileCache  catfile.Cache
	txManager     transaction.Manager

	conns *client.Pool

	disableFetchInternalRemoteErrors bool
}

// NewServer creates a new instance of a grpc RemoteServiceServer
func NewServer(
	cfg config.Cfg,
	rs *rubyserver.Server,
	locator storage.Locator,
	gitCmdFactory git.CommandFactory,
	catfileCache catfile.Cache,
	txManager transaction.Manager,
) gitalypb.RemoteServiceServer {
	disableFetchInternalRemoteErrors, err := env.GetBool("GITALY_DISABLE_FETCH_INTERNAL_REMOTE_ERRORS", false)
	if err != nil {
		panic(err)
	}

	return &server{
		cfg:           cfg,
		ruby:          rs,
		locator:       locator,
		gitCmdFactory: gitCmdFactory,
		catfileCache:  catfileCache,
		txManager:     txManager,
		conns: client.NewPoolWithOptions(
			client.WithDialer(client.HealthCheckDialer(client.DialContext)),
			client.WithDialOptions(client.FailOnNonTempDialError()...),
		),
		disableFetchInternalRemoteErrors: disableFetchInternalRemoteErrors,
	}
}

func (s *server) localrepo(repo repository.GitRepo) *localrepo.Repo {
	return localrepo.New(s.gitCmdFactory, s.catfileCache, repo, s.cfg)
}
