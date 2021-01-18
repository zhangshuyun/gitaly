package ref

import (
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

type server struct {
	cfg     config.Cfg
	locator storage.Locator
}

// NewServer creates a new instance of a grpc RefServer
func NewServer(cfg config.Cfg, locator storage.Locator) gitalypb.RefServiceServer {
	return &server{cfg: cfg, locator: locator}
}
