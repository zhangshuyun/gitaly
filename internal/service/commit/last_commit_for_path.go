package commit

import (
	"fmt"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"golang.org/x/net/context"
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
	if err != nil {
		return nil, err
	}

	return &gitalypb.LastCommitForPathResponse{Commit: commit}, nil
}

func validateLastCommitForPathRequest(in *gitalypb.LastCommitForPathRequest) error {
	if len(in.Revision) == 0 {
		return fmt.Errorf("empty Revision")
	}
	return nil
}
