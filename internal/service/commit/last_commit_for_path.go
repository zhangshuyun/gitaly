package commit

import (
	"context"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) LastCommitForPath(ctx context.Context, in *gitalypb.LastCommitForPathRequest) (*gitalypb.LastCommitForPathResponse, error) {
	if err := validateLastCommitForPathRequest(in); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "LastCommitForPath: %v", err)
	}

	path := string(in.GetPath())
	if len(path) == 0 || path == "/" {
		path = "."
	}

	commit, err := log.LastCommitForPath(ctx, in.GetRepository(), string(in.GetRevision()), path)
	if log.IsNotFound(err) {
		return &gitalypb.LastCommitForPathResponse{}, nil
	}

	return &gitalypb.LastCommitForPathResponse{Commit: commit}, err
}

func validateLastCommitForPathRequest(in *gitalypb.LastCommitForPathRequest) error {
	if err := git.ValidateRevision(in.Revision); err != nil {
		return helper.ErrInvalidArgument(err)
	}
	return nil
}
