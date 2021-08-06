package operations

import (
	"context"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/remoterepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func (s *Server) UserRevert(ctx context.Context, req *gitalypb.UserRevertRequest) (*gitalypb.UserRevertResponse, error) {
	if err := validateCherryPickOrRevertRequest(req); err != nil {
		return nil, helper.ErrInvalidArgument(err)
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
		return nil, helper.ErrInternalf("get path: %w", err)
	}

	var mainline uint
	if len(req.Commit.ParentIds) > 1 {
		mainline = 1
	}

	authorDate, err := dateFromProto(req)
	if err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	newrev, err := s.git2go.Revert(ctx, quarantineRepo, git2go.RevertCommand{
		Repository: repoPath,
		AuthorName: string(req.User.Name),
		AuthorMail: string(req.User.Email),
		AuthorDate: authorDate,
		Message:    string(req.Message),
		Ours:       startRevision.String(),
		Revert:     req.Commit.Id,
		Mainline:   mainline,
	})
	if err != nil {
		if errors.As(err, &git2go.HasConflictsError{}) {
			return &gitalypb.UserRevertResponse{
				CreateTreeError:     err.Error(),
				CreateTreeErrorCode: gitalypb.UserRevertResponse_CONFLICT,
			}, nil
		} else if errors.As(err, &git2go.EmptyError{}) {
			return &gitalypb.UserRevertResponse{
				CreateTreeError:     err.Error(),
				CreateTreeErrorCode: gitalypb.UserRevertResponse_EMPTY,
			}, nil
		} else if errors.Is(err, git2go.ErrInvalidArgument) {
			return nil, helper.ErrInvalidArgument(err)
		} else {
			return nil, helper.ErrInternalf("revert command: %w", err)
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
			return &gitalypb.UserRevertResponse{
				CommitError: "Branch diverged",
			}, nil
		}
	}

	if err := s.updateReferenceWithHooks(ctx, req.GetRepository(), req.User, quarantineDir, referenceName, newrev, oldrev); err != nil {
		var preReceiveError updateref.PreReceiveError
		if errors.As(err, &preReceiveError) {
			return &gitalypb.UserRevertResponse{
				PreReceiveError: preReceiveError.Message,
			}, nil
		}

		return nil, fmt.Errorf("update reference with hooks: %w", err)
	}

	return &gitalypb.UserRevertResponse{
		BranchUpdate: &gitalypb.OperationBranchUpdate{
			CommitId:      newrev.String(),
			BranchCreated: branchCreated,
			RepoCreated:   !repoHadBranches,
		},
	}, nil
}

type requestFetchingStartRevision interface {
	GetBranchName() []byte
	GetStartRepository() *gitalypb.Repository
	GetStartBranchName() []byte
}

func (s *Server) fetchStartRevision(
	ctx context.Context,
	localRepo *localrepo.Repo,
	req requestFetchingStartRevision,
) (git.ObjectID, error) {
	startBranchName := req.GetStartBranchName()
	if len(startBranchName) == 0 {
		startBranchName = req.GetBranchName()
	}

	var remoteRepo git.Repository = localRepo
	if startRepository := req.GetStartRepository(); startRepository != nil {
		var err error
		remoteRepo, err = remoterepo.New(ctx, startRepository, s.conns)
		if err != nil {
			return "", helper.ErrInternal(err)
		}
	}

	startRevision, err := remoteRepo.ResolveRevision(ctx, git.Revision(fmt.Sprintf("%s^{commit}", startBranchName)))
	if err != nil {
		return "", helper.ErrInvalidArgumentf("resolve start ref: %w", err)
	}

	if req.GetStartRepository() == nil {
		return startRevision, nil
	}

	_, err = localRepo.ResolveRevision(ctx, startRevision.Revision()+"^{commit}")
	if errors.Is(err, git.ErrReferenceNotFound) {
		if err := localRepo.FetchInternal(
			ctx,
			req.GetStartRepository(),
			[]string{startRevision.String()},
			localrepo.FetchOpts{Tags: localrepo.FetchOptsTagsNone},
		); err != nil {
			return "", helper.ErrInternalf("fetch start: %w", err)
		}
	} else if err != nil {
		return "", helper.ErrInvalidArgumentf("resolve start: %w", err)
	}

	return startRevision, nil
}
