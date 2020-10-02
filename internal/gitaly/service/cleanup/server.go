package cleanup

import (
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
)

type server struct {
}

// NewServer creates a new instance of a grpc CleanupServer
func NewServer() gitalypb.CleanupServiceServer {
	return &server{}
}
