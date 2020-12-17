package commit

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func (s *server) FindCommit(ctx context.Context, in *gitalypb.FindCommitRequest) (*gitalypb.FindCommitResponse, error) {
	revision := in.GetRevision()
	if err := git.ValidateRevision(revision); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	repo := in.GetRepository()

	commit, err := log.GetCommit(ctx, repo, string(revision))
	if log.IsNotFound(err) {
		return &gitalypb.FindCommitResponse{}, nil
	}

	return &gitalypb.FindCommitResponse{Commit: commit}, err
}
