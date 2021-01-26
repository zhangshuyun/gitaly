package cleanup

import (
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

type server struct {
	cfg config.Cfg
}

// NewServer creates a new instance of a grpc CleanupServer
func NewServer(cfg config.Cfg) gitalypb.CleanupServiceServer {
	return &server{cfg: cfg}
}
