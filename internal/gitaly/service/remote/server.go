package remote

import (
	"gitlab.com/gitlab-org/gitaly/v13/client"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v13/internal/storage"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
)

type server struct {
	ruby    *rubyserver.Server
	locator storage.Locator

	conns *client.Pool
}

// NewServer creates a new instance of a grpc RemoteServiceServer
func NewServer(rs *rubyserver.Server, locator storage.Locator) gitalypb.RemoteServiceServer {
	return &server{
		ruby:    rs,
		locator: locator,
		conns:   client.NewPool(),
	}
}
