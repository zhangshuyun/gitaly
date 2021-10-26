package objectpool

import (
	"context"
	"errors"
	"fmt"

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

func (s *server) UnlinkRepositoryFromObjectPool(ctx context.Context, req *gitalypb.UnlinkRepositoryFromObjectPoolRequest) (*gitalypb.UnlinkRepositoryFromObjectPoolResponse, error) {
	if req.GetRepository() == nil {
		return nil, helper.ErrInvalidArgument(errors.New("no repository"))
	}

	pool, err := s.poolForRequest(req)
	if err != nil {
		return nil, helper.ErrInternal(err)
	}

	if !pool.Exists() {
		return nil, helper.ErrNotFound(fmt.Errorf("pool repository not found: %s", pool.FullPath()))
	}

	if err := pool.Unlink(ctx, req.GetRepository()); err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.UnlinkRepositoryFromObjectPoolResponse{}, nil
}
