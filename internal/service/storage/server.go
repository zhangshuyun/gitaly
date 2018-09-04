package storage

import (
	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type server struct{}

func (s *server) ListDirectories(*pb.ListDirectoriesRequest, pb.StorageService_ListDirectoriesServer) error {
	return grpc.Errorf(codes.Unimplemented, "ListDirectories unimplemented")
}

// NewServer creates a new instance of a gRPC storage server
func NewServer() pb.StorageServiceServer {
	return &server{}
}
