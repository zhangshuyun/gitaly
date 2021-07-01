package operations

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const userUpdateSubmoduleName = "UserUpdateSubmodule"

func (s *Server) UserUpdateSubmodule(ctx context.Context, req *gitalypb.UserUpdateSubmoduleRequest) (*gitalypb.UserUpdateSubmoduleResponse, error) {
	if err := validateUserUpdateSubmoduleRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, userUpdateSubmoduleName+": %v", err)
	}

	return s.userUpdateSubmodule(ctx, req)
}

func validateUserUpdateSubmoduleRequest(req *gitalypb.UserUpdateSubmoduleRequest) error {
	if req.GetRepository() == nil {
		return fmt.Errorf("empty Repository")
	}

	if req.GetUser() == nil {
		return fmt.Errorf("empty User")
	}

	if req.GetCommitSha() == "" {
		return fmt.Errorf("empty CommitSha")
	}

	if match, err := regexp.MatchString(`\A[0-9a-f]{40}\z`, req.GetCommitSha()); !match || err != nil {
		return fmt.Errorf("invalid CommitSha")
	}

	if len(req.GetBranch()) == 0 {
		return fmt.Errorf("empty Branch")
	}

	if len(req.GetSubmodule()) == 0 {
		return fmt.Errorf("empty Submodule")
	}

	if len(req.GetCommitMessage()) == 0 {
		return fmt.Errorf("empty CommitMessage")
	}

	return nil
}

func (s *Server) userUpdateSubmodule(ctx context.Context, req *gitalypb.UserUpdateSubmoduleRequest) (*gitalypb.UserUpdateSubmoduleResponse, error) {
	repo := s.localrepo(req.GetRepository())
	branches, err := repo.GetBranches(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s: get branches: %w", userUpdateSubmoduleName, err)
	}
	if len(branches) == 0 {
		return &gitalypb.UserUpdateSubmoduleResponse{
			CommitError: "Repository is empty",
		}, nil
	}

	referenceName := git.NewReferenceNameFromBranchName(string(req.GetBranch()))

	branchOID, err := repo.ResolveRevision(ctx, referenceName.Revision())
	if err != nil {
		if errors.Is(err, git.ErrReferenceNotFound) {
			return nil, helper.ErrInvalidArgumentf("Cannot find branch")
		}
		return nil, fmt.Errorf("%s: get branch: %w", userUpdateSubmoduleName, err)
	}

	repoPath, err := s.locator.GetRepoPath(req.GetRepository())
	if err != nil {
		return nil, fmt.Errorf("%s: locate repo: %w", userUpdateSubmoduleName, err)
	}

	authorDate, err := dateFromProto(req)
	if err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	result, err := git2go.SubmoduleCommand{
		Repository: repoPath,
		AuthorMail: string(req.GetUser().GetEmail()),
		AuthorName: string(req.GetUser().GetName()),
		AuthorDate: authorDate,
		Branch:     string(req.GetBranch()),
		CommitSHA:  req.GetCommitSha(),
		Submodule:  string(req.GetSubmodule()),
		Message:    string(req.GetCommitMessage()),
	}.Run(ctx, s.cfg)
	if err != nil {
		errStr := strings.TrimPrefix(err.Error(), "submodule: ")
		errStr = strings.TrimSpace(errStr)

		var resp *gitalypb.UserUpdateSubmoduleResponse
		for _, legacyErr := range []string{
			git2go.LegacyErrPrefixInvalidBranch,
			git2go.LegacyErrPrefixInvalidSubmodulePath,
			git2go.LegacyErrPrefixFailedCommit,
		} {
			if strings.HasPrefix(errStr, legacyErr) {
				resp = &gitalypb.UserUpdateSubmoduleResponse{
					CommitError: legacyErr,
				}
				ctxlogrus.
					Extract(ctx).
					WithError(err).
					Error(userUpdateSubmoduleName + ": git2go subcommand failure")
				break
			}
		}
		if strings.Contains(errStr, "is already at") {
			resp = &gitalypb.UserUpdateSubmoduleResponse{
				CommitError: errStr,
			}
		}
		if resp != nil {
			return resp, nil
		}
		return nil, fmt.Errorf("%s: submodule subcommand: %w", userUpdateSubmoduleName, err)
	}

	commitID, err := git.NewObjectIDFromHex(result.CommitID)
	if err != nil {
		return nil, helper.ErrInvalidArgumentf("cannot parse commit ID: %w", err)
	}

	if err := s.updateReferenceWithHooks(
		ctx,
		req.GetRepository(),
		req.GetUser(),
		referenceName,
		commitID,
		branchOID,
	); err != nil {
		var preReceiveError updateref.PreReceiveError
		if errors.As(err, &preReceiveError) {
			return &gitalypb.UserUpdateSubmoduleResponse{
				PreReceiveError: preReceiveError.Error(),
			}, nil
		}

		var updateRefError updateref.Error
		if errors.As(err, &updateRefError) {
			return &gitalypb.UserUpdateSubmoduleResponse{
				CommitError: err.Error(),
			}, nil
		}

		return nil, err
	}

	return &gitalypb.UserUpdateSubmoduleResponse{
		BranchUpdate: &gitalypb.OperationBranchUpdate{
			CommitId:      result.CommitID,
			BranchCreated: false,
			RepoCreated:   false,
		},
	}, nil
}
