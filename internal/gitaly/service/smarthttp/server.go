package smarthttp

import (
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/internal/cache"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

type server struct {
	locator                    storage.Locator
	packfileNegotiationMetrics *prometheus.CounterVec
	infoRefCache               infoRefCache
}

// NewServer creates a new instance of a grpc SmartHTTPServer
func NewServer(locator storage.Locator, serverOpts ...ServerOpt) gitalypb.SmartHTTPServiceServer {
	s := &server{
		locator: locator,
		packfileNegotiationMetrics: prometheus.NewCounterVec(
			prometheus.CounterOpts{},
			[]string{"git_negotiation_feature"},
		),
		infoRefCache: newInfoRefCache(cache.NewStreamDB(cache.NewLeaseKeyer(locator))),
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
