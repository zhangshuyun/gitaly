package remote

import (
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

type server struct {
	ruby *rubyserver.Server
	gitalypb.UnimplementedRemoteServiceServer
	storages config.Storages
}

// NewServer creates a new instance of a grpc RemoteServiceServer
func NewServer(rs *rubyserver.Server, storages config.Storages) gitalypb.RemoteServiceServer {
	return &server{ruby: rs, storages: storages}
}
