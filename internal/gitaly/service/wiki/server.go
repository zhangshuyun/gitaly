package wiki

import (
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
)

type server struct {
	ruby *rubyserver.Server
}

// NewServer creates a new instance of a grpc WikiServiceServer
func NewServer(rs *rubyserver.Server) gitalypb.WikiServiceServer {
	return &server{ruby: rs}
}
