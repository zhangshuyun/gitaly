package hook

import (
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	gitalyhook "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/streamcache"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

type server struct {
	gitalypb.UnimplementedHookServiceServer
	cfg              config.Cfg
	manager          gitalyhook.Manager
	gitCmdFactory    git.CommandFactory
	packObjectsCache streamcache.Cache
}

// NewServer creates a new instance of a gRPC namespace server
func NewServer(cfg config.Cfg, manager gitalyhook.Manager, gitCmdFactory git.CommandFactory, packObjectsCache streamcache.Cache) gitalypb.HookServiceServer {
	srv := &server{
		cfg:              cfg,
		manager:          manager,
		gitCmdFactory:    gitCmdFactory,
		packObjectsCache: packObjectsCache,
	}

	return srv
}
