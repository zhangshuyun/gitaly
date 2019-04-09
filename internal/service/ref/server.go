package ref

import (
	"context"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
)

type server struct {
	*rubyserver.Server
}

// NewServer creates a new instance of a grpc RefServer
func NewServer(rs *rubyserver.Server) gitalypb.RefServiceServer {

	return &server{rs}
}

func (*server) PackRefs(_ context.Context, _ *gitalypb.PackRefsRequest) (*gitalypb.PackRefsResponse, error) {
	return nil, helper.Unimplemented
}
