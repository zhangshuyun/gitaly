package repository

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

// PruneUnreachableObjects prunes objects which aren't reachable from any of its references. To
// ensure that concurrently running commands do not reference those objects anymore when we execute
// the prune we enforce a grace-period: objects will only be pruned if they haven't been accessed
// for at least 30 minutes.
func (s *server) PruneUnreachableObjects(
	ctx context.Context,
	request *gitalypb.PruneUnreachableObjectsRequest,
) (*gitalypb.PruneUnreachableObjectsResponse, error) {
	if request.GetRepository() == nil {
		return nil, helper.ErrInvalidArgumentf("missing repository")
	}

	repo := s.localrepo(request.GetRepository())

	// Verify that the repository exists on-disk such that we can return a proper gRPC code in
	// case it doesn't.
	if _, err := repo.Path(); err != nil {
		return nil, err
	}

	if err := repo.ExecAndWait(ctx, git.SubCmd{
		Name: "prune",
		Flags: []git.Option{
			git.ValueFlag{Name: "--expire", Value: "30.minutes.ago"},
		},
	}); err != nil {
		return nil, helper.ErrInternalf("pruning objects: %w", err)
	}

	stats.LogObjectsInfo(ctx, repo)

	return &gitalypb.PruneUnreachableObjectsResponse{}, nil
}
