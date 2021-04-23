package smarthttp

import (
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/cache"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/storage"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

type server struct {
	gitalypb.UnimplementedSmartHTTPServiceServer
	cfg                        config.Cfg
	locator                    storage.Locator
	gitCmdFactory              git.CommandFactory
	packfileNegotiationMetrics *prometheus.CounterVec
	infoRefCache               infoRefCache
}

// NewServer creates a new instance of a grpc SmartHTTPServer
func NewServer(cfg config.Cfg, locator storage.Locator, gitCmdFactory git.CommandFactory,
	cache *cache.Cache, serverOpts ...ServerOpt) gitalypb.SmartHTTPServiceServer {
	s := &server{
		cfg:           cfg,
		locator:       locator,
		gitCmdFactory: gitCmdFactory,
		packfileNegotiationMetrics: prometheus.NewCounterVec(
			prometheus.CounterOpts{},
			[]string{"git_negotiation_feature"},
		),
		infoRefCache: newInfoRefCache(cache),
	}

	for _, serverOpt := range serverOpts {
		serverOpt(s)
	}

	return s
}

// ServerOpt is a self referential option for server
type ServerOpt func(s *server)

func WithPackfileNegotiationMetrics(c *prometheus.CounterVec) ServerOpt {
	return func(s *server) {
		s.packfileNegotiationMetrics = c
	}
}
