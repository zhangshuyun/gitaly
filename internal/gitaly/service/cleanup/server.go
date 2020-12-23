package cleanup

import (
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

type server struct {
	cfg     config.Cfg
	locator storage.Locator
}

// NewServer creates a new instance of a grpc CleanupServer
func NewServer(cfg config.Cfg, locator storage.Locator) gitalypb.CleanupServiceServer {
	return &server{cfg: cfg, locator: locator}
}
