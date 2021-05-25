package server

import (
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

type server struct {
	gitCmdFactory git.CommandFactory
	storages      []config.Storage
}

// NewServer creates a new instance of a grpc ServerServiceServer
func NewServer(gitCmdFactory git.CommandFactory, storages []config.Storage) gitalypb.ServerServiceServer {
	return &server{gitCmdFactory: gitCmdFactory, storages: storages}
}
