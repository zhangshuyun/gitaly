package hook

import (
	gitalyhook "gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
)

type server struct {
	manager gitalyhook.Manager
}

// NewServer creates a new instance of a gRPC namespace server
func NewServer(manager gitalyhook.Manager) gitalypb.HookServiceServer {
	return &server{
		manager: manager,
	}
}
