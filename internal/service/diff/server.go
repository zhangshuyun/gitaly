package diff

import (
	pb "gitlab.com/gitlab-org/gitaly-proto/go"
)

const msgSizeThreshold = 1024

type server struct {
	MsgSizeThreshold int
}

// NewServer creates a new instance of a gRPC DiffServer
func NewServer() pb.DiffServer {
	return &server{MsgSizeThreshold: msgSizeThreshold}
}
