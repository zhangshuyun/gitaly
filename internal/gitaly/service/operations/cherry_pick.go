package operations

import (
	"context"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) UserCherryPick(ctx context.Context, req *gitalypb.UserCherryPickRequest) (*gitalypb.UserCherryPickResponse, error) {
	if err := validateCherryPickOrRevertRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "UserCherryPick: %v", err)
	}

	if featureflag.IsEnabled(ctx, featureflag.GoUserCherryPick) {
		return s.userCherryPick(ctx, req)
	}

	client, err := s.ruby.OperationServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, s.locator, req.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.UserCherryPick(clientCtx, req)
}

func (s *Server) userCherryPick(ctx context.Context, req *gitalypb.UserCherryPickRequest) (*gitalypb.UserCherryPickResponse, error) {
	startRevision, err := s.fetchStartRevision(ctx, req)
	if err != nil {
		return nil, err
	}

	localRepo := localrepo.New(s.gitCmdFactory, req.Repository, s.cfg)
	repoHadBranches, err := localRepo.HasBranches(ctx)
	if err != nil {
		return nil, err
	}

	repoPath, err := s.locator.GetPath(req.Repository)
	if err != nil {
		return nil, err
	}

	var mainline uint
	if len(req.Commit.ParentIds) > 1 {
		mainline = 1
	}

	newrev, err := git2go.CherryPickCommand{
		Repository: repoPath,
		AuthorName: string(req.User.Name),
		AuthorMail: string(req.User.Email),
		Message:    string(req.Message),
		Commit:     req.Commit.Id,
		Ours:       startRevision,
		Mainline:   mainline,
	}.Run(ctx, s.cfg)
	if err != nil {
		switch {
		case errors.As(err, &git2go.HasConflictsError{}):
			return &gitalypb.UserCherryPickResponse{
				CreateTreeError:     err.Error(),
				CreateTreeErrorCode: gitalypb.UserCherryPickResponse_CONFLICT,
			}, nil
		case errors.Is(err, git2go.ErrInvalidArgument):
			return nil, helper.ErrInvalidArgument(err)
		default:
			return nil, helper.ErrInternalf("cherry-pick command: %w", err)
		}
	}

	branch := "refs/heads/" + text.ChompBytes(req.BranchName)

	branchCreated := false
	oldrev, err := localRepo.ResolveRevision(ctx, git.Revision(fmt.Sprintf("%s^{commit}", branch)))
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
		ancestor, err := s.isAncestor(ctx, req.Repository, oldrev.String(), newrev)
		if err != nil {
			return nil, err
		}
		if !ancestor {
			return &gitalypb.UserCherryPickResponse{
				CommitError: "Branch diverged",
			}, nil
		}
	}

	if err := s.updateReferenceWithHooks(ctx, req.Repository, req.User, branch, newrev, oldrev.String()); err != nil {
		if errors.As(err, &preReceiveError{}) {
			return &gitalypb.UserCherryPickResponse{
				PreReceiveError: err.Error(),
			}, err
		}

		return nil, fmt.Errorf("update reference with hooks: %w", err)
	}

	return &gitalypb.UserCherryPickResponse{
		BranchUpdate: &gitalypb.OperationBranchUpdate{
			CommitId:      newrev,
			BranchCreated: branchCreated,
			RepoCreated:   !repoHadBranches,
		},
	}, nil
}
