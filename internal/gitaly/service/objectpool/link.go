package objectpool

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) LinkRepositoryToObjectPool(ctx context.Context, req *gitalypb.LinkRepositoryToObjectPoolRequest) (*gitalypb.LinkRepositoryToObjectPoolResponse, error) {
	if req.GetRepository() == nil {
		return nil, status.Error(codes.InvalidArgument, "no repository")
	}

	pool, err := s.poolForRequest(req)
	if err != nil {
		return nil, err
	}

	if err := pool.Init(ctx); err != nil {
		return nil, helper.ErrInternal(err)
	}

	if err := pool.Link(ctx, req.GetRepository()); err != nil {
		return nil, helper.ErrInternal(helper.SanitizeError(err))
	}

	return &gitalypb.LinkRepositoryToObjectPoolResponse{}, nil
}
