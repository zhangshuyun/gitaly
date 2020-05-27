package repository

import (
	"context"
	"sync"

	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

type server struct {
	ruby *rubyserver.Server
	gitalypb.UnimplementedRepositoryServiceServer
	connsByAddress       map[string]*cachedConn
	connsMtx             sync.RWMutex
	internalGitalySocket string
}

// NewServer creates a new instance of a gRPC repo server
func NewServer(rs *rubyserver.Server, internalGitalySocket string) gitalypb.RepositoryServiceServer {
	return &server{ruby: rs, connsByAddress: make(map[string]*cachedConn), internalGitalySocket: internalGitalySocket}
}

func (*server) FetchHTTPRemote(context.Context, *gitalypb.FetchHTTPRemoteRequest) (*gitalypb.FetchHTTPRemoteResponse, error) {
	return nil, helper.Unimplemented
}
