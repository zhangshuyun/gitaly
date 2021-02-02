package conflicts

import (
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

type server struct {
	ruby          *rubyserver.Server
	cfg           config.Cfg
	locator       storage.Locator
	gitCmdFactory git.CommandFactory
	pool          *client.Pool
}

// NewServer creates a new instance of a grpc ConflictsServer
func NewServer(rs *rubyserver.Server, cfg config.Cfg, locator storage.Locator, gitCmdFactory git.CommandFactory) gitalypb.ConflictsServiceServer {
	return &server{
		ruby:          rs,
		cfg:           cfg,
		locator:       locator,
		gitCmdFactory: gitCmdFactory,
		pool: client.NewPoolWithOptions(
			client.WithDialer(client.HealthCheckDialer(client.DialContext)),
			client.WithDialOptions(client.FailOnNonTempDialError()...),
		),
	}
}
