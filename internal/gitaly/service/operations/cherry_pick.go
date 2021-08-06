package operations

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) UserCherryPick(ctx context.Context, req *gitalypb.UserCherryPickRequest) (*gitalypb.UserCherryPickResponse, error) {
	if err := validateCherryPickOrRevertRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "UserCherryPick: %v", err)
	}

	quarantineDir, quarantineRepo, err := s.quarantinedRepo(ctx, req.GetRepository(), featureflag.Quarantine)
	if err != nil {
		return nil, err
	}

	startRevision, err := s.fetchStartRevision(ctx, quarantineRepo, req)
	if err != nil {
		return nil, err
	}

	repoHadBranches, err := quarantineRepo.HasBranches(ctx)
	if err != nil {
		return nil, err
	}

	repoPath, err := quarantineRepo.Path()
	if err != nil {
		return nil, err
	}

	var mainline uint
	if len(req.Commit.ParentIds) > 1 {
		mainline = 1
	}

	committerDate := time.Now()
	if req.Timestamp != nil {
		committerDate = req.Timestamp.AsTime()
	}

	newrev, err := s.git2go.CherryPick(ctx, quarantineRepo, git2go.CherryPickCommand{
		Repository:    repoPath,
		CommitterName: string(req.User.Name),
		CommitterMail: string(req.User.Email),
		CommitterDate: committerDate,
		Message:       string(req.Message),
		Commit:        req.Commit.Id,
		Ours:          startRevision.String(),
		Mainline:      mainline,
	})
	if err != nil {
		switch {
		case errors.As(err, &git2go.HasConflictsError{}):
			return &gitalypb.UserCherryPickResponse{
				CreateTreeError:     err.Error(),
				CreateTreeErrorCode: gitalypb.UserCherryPickResponse_CONFLICT,
			}, nil
		case errors.As(err, &git2go.EmptyError{}):
			return &gitalypb.UserCherryPickResponse{
				CreateTreeError:     err.Error(),
				CreateTreeErrorCode: gitalypb.UserCherryPickResponse_EMPTY,
			}, nil
		case errors.Is(err, git2go.ErrInvalidArgument):
			return nil, helper.ErrInvalidArgument(err)
		default:
			return nil, helper.ErrInternalf("cherry-pick command: %w", err)
		}
	}

	referenceName := git.NewReferenceNameFromBranchName(string(req.BranchName))

	branchCreated := false
	oldrev, err := quarantineRepo.ResolveRevision(ctx, referenceName.Revision()+"^{commit}")
	if errors.Is(err, git.ErrReferenceNotFound) {
		branchCreated = true
		oldrev = git.ZeroOID
	} else if err != nil {
		return nil, helper.ErrInvalidArgumentf("resolve ref: %w", err)
	}

	if req.DryRun {
		newrev = startRevision
	}

	if !branchCreated {
		ancestor, err := quarantineRepo.IsAncestor(ctx, oldrev.Revision(), newrev.Revision())
		if err != nil {
			return nil, err
		}
		if !ancestor {
			return &gitalypb.UserCherryPickResponse{
				CommitError: "Branch diverged",
			}, nil
		}
	}

	if err := s.updateReferenceWithHooks(ctx, req.GetRepository(), req.User, quarantineDir, referenceName, newrev, oldrev); err != nil {
		if errors.As(err, &updateref.PreReceiveError{}) {
			return &gitalypb.UserCherryPickResponse{
				PreReceiveError: err.Error(),
			}, nil
		}

		return nil, fmt.Errorf("update reference with hooks: %w", err)
	}

	return &gitalypb.UserCherryPickResponse{
		BranchUpdate: &gitalypb.OperationBranchUpdate{
			CommitId:      newrev.String(),
			BranchCreated: branchCreated,
			RepoCreated:   !repoHadBranches,
		},
	}, nil
}
