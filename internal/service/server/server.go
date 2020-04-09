package server

import (
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

type server struct {
	storages []config.Storage
	gitalypb.UnimplementedServerServiceServer
}

// NewServer creates a new instance of a grpc ServerServiceServer
func NewServer(storages []config.Storage) gitalypb.ServerServiceServer {
	return &server{storages: storages}
}
