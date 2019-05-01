package commit

import (
	"context"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) FindCommit(ctx context.Context, in *gitalypb.FindCommitRequest) (*gitalypb.FindCommitResponse, error) {
	revision := in.GetRevision()
	if err := git.ValidateRevision(revision); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "FindCommit: revision: %v", err)
	}

	repo := in.GetRepository()

	commit, err := log.GetCommit(ctx, repo, string(revision))
	if log.IsNotFound(err) {
		return &gitalypb.FindCommitResponse{}, nil
	}

	return &gitalypb.FindCommitResponse{Commit: commit}, err
}
