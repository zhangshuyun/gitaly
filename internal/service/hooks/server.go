package hook

import "gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"

type server struct{}

// NewServer creates a new instance of a gRPC namespace server
func NewServer() gitalypb.HookServiceServer {
	return &server{}
}
