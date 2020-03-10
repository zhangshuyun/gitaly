package repository

import (
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"
)

type Server struct {
	nodeManager nodes.Manager
	ds          datastore.ReplJobsDatastore
}

func NewServer(nodeMgr nodes.Manager, jobsDS datastore.ReplJobsDatastore) *Server {
	return &Server{
		nodeManager: nodeMgr,
		ds:          jobsDS,
	}
}
