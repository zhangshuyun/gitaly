package smarthttp

import (
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
)

type server struct {
	packfileNegotiationMetrics *prometheus.CounterVec
}

// NewServer creates a new instance of a grpc SmartHTTPServer
func NewServer(serverOpts ...ServerOpt) gitalypb.SmartHTTPServiceServer {
	s := &server{
		packfileNegotiationMetrics: prometheus.NewCounterVec(
			prometheus.CounterOpts{},
			[]string{"git_negotiation_feature"},
		),
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
