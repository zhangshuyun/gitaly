package commit

import (
	"context"
	"errors"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func (s *server) FindCommit(ctx context.Context, in *gitalypb.FindCommitRequest) (*gitalypb.FindCommitResponse, error) {
	revision := in.GetRevision()
	if err := git.ValidateRevision(revision); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	repo := s.localrepo(in.GetRepository())

	var opts []localrepo.ReadCommitOpt
	if in.GetTrailers() {
		opts = []localrepo.ReadCommitOpt{localrepo.WithTrailers()}
	}

	commit, err := repo.ReadCommit(ctx, git.Revision(revision), opts...)
	if err != nil {
		if errors.Is(err, localrepo.ErrObjectNotFound) {
			return &gitalypb.FindCommitResponse{}, nil
		}
		return &gitalypb.FindCommitResponse{}, err
	}

	return &gitalypb.FindCommitResponse{Commit: commit}, nil
}
