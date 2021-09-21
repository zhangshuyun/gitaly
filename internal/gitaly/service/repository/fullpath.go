package repository

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

const fullPathKey = "gitlab.fullpath"

// SetFullPath writes the provided path value into the repository's gitconfig under the
// "gitlab.fullpath" key.
func (s *server) SetFullPath(
	ctx context.Context,
	request *gitalypb.SetFullPathRequest,
) (*gitalypb.SetFullPathResponse, error) {
	if request.GetRepository() == nil {
		return nil, helper.ErrInvalidArgumentf("empty Repository")
	}

	if len(request.GetPath()) == 0 {
		return nil, helper.ErrInvalidArgumentf("no path provided")
	}

	repo := s.localrepo(request.GetRepository())

	if featureflag.TxFileLocking.IsEnabled(ctx) {
		if err := repo.SetConfig(ctx, fullPathKey, request.GetPath(), s.txManager); err != nil {
			return nil, helper.ErrInternalf("setting config: %w", err)
		}

		return &gitalypb.SetFullPathResponse{}, nil
	}

	if err := s.voteOnConfig(ctx, request.GetRepository()); err != nil {
		return nil, helper.ErrInternalf("preimage vote on config: %w", err)
	}

	if err := repo.Config().Set(ctx, fullPathKey, request.GetPath()); err != nil {
		return nil, helper.ErrInternalf("writing config: %w", err)
	}

	if err := s.voteOnConfig(ctx, request.GetRepository()); err != nil {
		return nil, helper.ErrInternalf("postimage vote on config: %w", err)
	}

	return &gitalypb.SetFullPathResponse{}, nil
}
