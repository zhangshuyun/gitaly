package remote

import (
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

type server struct {
	cfg           config.Cfg
	ruby          *rubyserver.Server
	locator       storage.Locator
	gitCmdFactory git.CommandFactory

	conns *client.Pool
}

// NewServer creates a new instance of a grpc RemoteServiceServer
func NewServer(cfg config.Cfg, rs *rubyserver.Server, locator storage.Locator, gitCmdFactory git.CommandFactory) gitalypb.RemoteServiceServer {
	return &server{
		cfg:           cfg,
		ruby:          rs,
		locator:       locator,
		gitCmdFactory: gitCmdFactory,
		conns: client.NewPoolWithOptions(
			client.WithDialer(client.HealthCheckDialer(client.DialContext)),
			client.WithDialOptions(client.FailOnNonTempDialError()...),
		),
	}
}
