package info

import (
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/conn"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

// Server is a InfoService server
type Server struct {
	datastore datastore.ReplicasDatastore
	clientCC  *conn.ClientConnections
}

// NewServer creates a new instance of a grpc InfoServiceServer
func NewServer(conf config.Config, ds datastore.ReplicasDatastore, clientCC *conn.ClientConnections) gitalypb.InfoServiceServer {
	s := &Server{
		datastore: ds,
		clientCC:  clientCC,
	}

	return s
}
