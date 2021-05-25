package server

import (
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/service"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

// Server is a ServerService server
type Server struct {
	conf  config.Config
	conns service.Connections
}

// NewServer creates a new instance of a grpc ServerServiceServer
func NewServer(conf config.Config, conns service.Connections) gitalypb.ServerServiceServer {
	s := &Server{
		conf:  conf,
		conns: conns,
	}

	return s
}
