package repository

import (
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

type server struct {
	ruby          *rubyserver.Server
	conns         *client.Pool
	locator       storage.Locator
	txManager     transaction.Manager
	gitCmdFactory git.CommandFactory
	cfg           config.Cfg
	binDir        string
	loggingCfg    config.Logging
}

// NewServer creates a new instance of a gRPC repo server
func NewServer(
	cfg config.Cfg,
	rs *rubyserver.Server,
	locator storage.Locator,
	txManager transaction.Manager,
	gitCmdFactory git.CommandFactory,
) gitalypb.RepositoryServiceServer {
	return &server{
		ruby:          rs,
		locator:       locator,
		txManager:     txManager,
		gitCmdFactory: gitCmdFactory,
		conns: client.NewPoolWithOptions(
			client.WithDialer(client.HealthCheckDialer(client.DialContext)),
			client.WithDialOptions(client.FailOnNonTempDialError()...),
		),
		cfg:        cfg,
		binDir:     cfg.BinDir,
		loggingCfg: cfg.Logging,
	}
}
