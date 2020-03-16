package objectpool

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) CreateObjectPool(ctx context.Context, in *gitalypb.CreateObjectPoolRequest) (*gitalypb.CreateObjectPoolResponse, error) {
	if in.GetOrigin() == nil {
		return nil, status.Errorf(codes.InvalidArgument, "no origin repository")
	}

	pool, err := poolForRequest(in)
	if err != nil {
		return nil, err
	}

	if pool.Exists() {
		return nil, status.Errorf(codes.FailedPrecondition, "pool already exists at: %v", pool.GetRelativePath())
	}

	if err := pool.Create(ctx, in.GetOrigin()); err != nil {
		return nil, err
	}

	return &gitalypb.CreateObjectPoolResponse{}, nil
}

func (s *server) DeleteObjectPool(ctx context.Context, in *gitalypb.DeleteObjectPoolRequest) (*gitalypb.DeleteObjectPoolResponse, error) {
	pool, err := poolForRequest(in)
	if err != nil {
		return nil, err
	}

	if err := pool.Remove(ctx); err != nil {
		return nil, err
	}

	return &gitalypb.DeleteObjectPoolResponse{}, nil
}

type poolRequest interface {
	GetObjectPool() *gitalypb.ObjectPool
}

func poolForRequest(req poolRequest) (*objectpool.ObjectPool, error) {
	reqPool := req.GetObjectPool()

	poolRepo := reqPool.GetRepository()
	if poolRepo == nil {
		return nil, status.Errorf(codes.InvalidArgument, "no object pool repository")
	}

	return objectpool.NewObjectPool(poolRepo.GetStorageName(), poolRepo.GetRelativePath())
}
