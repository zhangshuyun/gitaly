package storage

import (
	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	helper "gitlab.com/gitlab-org/gitaly/internal/helper"
)

type server struct{}

// NewServer creates a new instance of a gRPC storage server
func NewServer() pb.StorageServiceServer {
	return &server{}
}

func (*server) ListDirectories(*pb.ListDirectoriesRequest, pb.StorageService_ListDirectoriesServer) error {
	return helper.Unimplemented
}
