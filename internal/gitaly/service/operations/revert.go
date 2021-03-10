package operations

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/golang/protobuf/ptypes"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/git/remoterepo"
	"gitlab.com/gitlab-org/gitaly/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func (s *Server) UserRevert(ctx context.Context, req *gitalypb.UserRevertRequest) (*gitalypb.UserRevertResponse, error) {
	if err := validateCherryPickOrRevertRequest(req); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}
	if featureflag.IsDisabled(ctx, featureflag.GoUserRevert) {
		return s.rubyUserRevert(ctx, req)
	}

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
		return nil, helper.ErrInternalf("get path: %w", err)
	}

	var mainline uint
	if len(req.Commit.ParentIds) > 1 {
		mainline = 1
	}

	authorDate := time.Now()
	if req.Timestamp != nil {
		authorDate, err = ptypes.Timestamp(req.Timestamp)
		if err != nil {
			return nil, helper.ErrInvalidArgument(err)
		}
	}

	newrev, err := git2go.RevertCommand{
		Repository: repoPath,
		AuthorName: string(req.User.Name),
		AuthorMail: string(req.User.Email),
		AuthorDate: authorDate,
		Message:    string(req.Message),
		Ours:       startRevision.String(),
		Revert:     req.Commit.Id,
		Mainline:   mainline,
	}.Run(ctx, s.cfg)
	if err != nil {
		if errors.As(err, &git2go.HasConflictsError{}) {
			return &gitalypb.UserRevertResponse{
				CreateTreeError:     err.Error(),
				CreateTreeErrorCode: gitalypb.UserRevertResponse_CONFLICT,
			}, nil
		} else if errors.Is(err, git2go.ErrInvalidArgument) {
			return nil, helper.ErrInvalidArgument(err)
		} else {
			return nil, helper.ErrInternalf("revert command: %w", err)
		}
	}

	referenceName := git.NewReferenceNameFromBranchName(string(req.BranchName))

	branchCreated := false
	oldrev, err := localRepo.ResolveRevision(ctx, referenceName.Revision()+"^{commit}")
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
		ancestor, err := s.isAncestor(ctx, req.Repository, oldrev, newrev)
		if err != nil {
			return nil, err
		}
		if !ancestor {
			return &gitalypb.UserRevertResponse{
				CommitError: "Branch diverged",
			}, nil
		}
	}

	if err := s.updateReferenceWithHooks(ctx, req.Repository, req.User, referenceName, newrev, oldrev); err != nil {
		var preReceiveError preReceiveError
		if errors.As(err, &preReceiveError) {
			return &gitalypb.UserRevertResponse{
				PreReceiveError: preReceiveError.message,
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

func (s *Server) rubyUserRevert(ctx context.Context, req *gitalypb.UserRevertRequest) (*gitalypb.UserRevertResponse, error) {
	client, err := s.ruby.OperationServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, s.locator, req.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.UserRevert(clientCtx, req)
}

type requestFetchingStartRevision interface {
	GetRepository() *gitalypb.Repository
	GetBranchName() []byte
	GetStartRepository() *gitalypb.Repository
	GetStartBranchName() []byte
}

func (s *Server) fetchStartRevision(ctx context.Context, req requestFetchingStartRevision) (git.ObjectID, error) {
	startBranchName := req.GetStartBranchName()
	if len(startBranchName) == 0 {
		startBranchName = req.GetBranchName()
	}

	startRepository := req.GetStartRepository()
	if startRepository == nil {
		startRepository = req.GetRepository()
	}

	remote, err := remoterepo.New(ctx, startRepository, s.conns)
	if err != nil {
		return "", helper.ErrInternal(err)
	}
	startRevision, err := remote.ResolveRevision(ctx, git.Revision(fmt.Sprintf("%s^{commit}", startBranchName)))
	if err != nil {
		return "", helper.ErrInvalidArgumentf("resolve start ref: %w", err)
	}

	if req.GetStartRepository() == nil {
		return startRevision, nil
	}

	localRepo := localrepo.New(s.gitCmdFactory, req.GetRepository(), s.cfg)

	_, err = localRepo.ResolveRevision(ctx, startRevision.Revision()+"^{commit}")
	if errors.Is(err, git.ErrReferenceNotFound) {
		if err := s.fetchRemoteObject(ctx, localRepo, req.GetStartRepository(), startRevision); err != nil {
			return "", helper.ErrInternalf("fetch start: %w", err)
		}
	} else if err != nil {
		return "", helper.ErrInvalidArgumentf("resolve start: %w", err)
	}

	return startRevision, nil
}
