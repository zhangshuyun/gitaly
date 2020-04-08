package objectpool

import (
	"context"
	"errors"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func (s *server) GetObjectPool(ctx context.Context, in *gitalypb.GetObjectPoolRequest) (*gitalypb.GetObjectPoolResponse, error) {
	if in.GetRepository() == nil {
		return nil, helper.ErrInternal(errors.New("repository is empty"))
	}

	objectPool, err := objectpool.FromRepo(in.GetRepository())

	if err != nil {
		ctxlogrus.Extract(ctx).
			WithError(err).
			WithField("storage", in.GetRepository().GetStorageName()).
			WithField("storage", in.GetRepository().GetRelativePath()).
			Warn("alternates file does not point to valid git repository")
	}

	if objectPool == nil {
		return &gitalypb.GetObjectPoolResponse{}, nil
	}

	return &gitalypb.GetObjectPoolResponse{
		ObjectPool: objectPool.ToProto(),
	}, nil
}
