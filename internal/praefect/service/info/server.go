package server

import (
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
)

// Server is a ServerService server
type Server struct {
	nodes     map[string]*grpc.ClientConn
	conf      config.Config
	datastore datastore.ReplicasDatastore
}

// NewServer creates a new instance of a grpc ServerServiceServer
func NewServer(conf config.Config, d datastore.ReplicasDatastore) gitalypb.ServerServiceServer {
	return &Server{
		nodes:     make(map[string]*grpc.ClientConn),
		conf:      conf,
		datastore: d,
	}
}
