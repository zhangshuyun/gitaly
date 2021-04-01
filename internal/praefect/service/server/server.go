package server

import (
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/service"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

// Server is a ServerService server
type Server struct {
	nodeMgr nodes.Manager
	conf    config.Config
	conns   service.Connections
}

// NewServer creates a new instance of a grpc ServerServiceServer
func NewServer(conf config.Config, nodeMgr nodes.Manager, conns service.Connections) gitalypb.ServerServiceServer {
	s := &Server{
		nodeMgr: nodeMgr,
		conf:    conf,
		conns:   conns,
	}

	return s
}
