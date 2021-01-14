package ref

import (
	"context"
	"errors"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) FindBranch(ctx context.Context, req *gitalypb.FindBranchRequest) (*gitalypb.FindBranchResponse, error) {
	refName := string(req.GetName())
	if len(refName) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Branch name cannot be empty")
	}

	if strings.HasPrefix(refName, "refs/heads/") {
		refName = strings.TrimPrefix(refName, "refs/heads/")
	} else if strings.HasPrefix(refName, "heads/") {
		refName = strings.TrimPrefix(refName, "heads/")
	}

	repo := req.GetRepository()

	branch, err := git.NewRepository(repo).GetBranch(ctx, refName)
	if err != nil {
		if errors.Is(err, git.ErrReferenceNotFound) {
			return &gitalypb.FindBranchResponse{}, nil
		}
		return nil, err
	}

	commit, err := log.GetCommit(ctx, s.locator, repo, git.Revision(branch.Target))
	if err != nil {
		return nil, err
	}

	return &gitalypb.FindBranchResponse{
		Branch: &gitalypb.Branch{
			Name:         []byte(refName),
			TargetCommit: commit,
		},
	}, nil
}
