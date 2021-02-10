package ref

import (
	"context"
	"errors"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) FindBranch(ctx context.Context, req *gitalypb.FindBranchRequest) (*gitalypb.FindBranchResponse, error) {
	if len(req.GetName()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Branch name cannot be empty")
	}

	repo := req.GetRepository()

	branchName := git.NewReferenceNameFromBranchName(string(req.GetName()))
	branchRef, err := localrepo.New(repo, s.cfg).GetReference(ctx, branchName)
	if err != nil {
		if errors.Is(err, git.ErrReferenceNotFound) {
			return &gitalypb.FindBranchResponse{}, nil
		}
		return nil, err
	}

	commit, err := log.GetCommit(ctx, s.gitCmdFactory, repo, git.Revision(branchRef.Target))
	if err != nil {
		return nil, err
	}

	branch, ok := branchName.Branch()
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "reference is not a branch")
	}

	return &gitalypb.FindBranchResponse{
		Branch: &gitalypb.Branch{
			Name:         []byte(branch),
			TargetCommit: commit,
		},
	}, nil
}
