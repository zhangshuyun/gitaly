package operations

import (
	"context"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) UserCreateBranch(ctx context.Context, req *gitalypb.UserCreateBranchRequest) (*gitalypb.UserCreateBranchResponse, error) {
	if len(req.BranchName) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Bad Request (empty branch name)")
	}

	if req.User == nil {
		return nil, status.Errorf(codes.InvalidArgument, "empty user")
	}

	if len(req.StartPoint) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "empty start point")
	}

	repo := localrepo.New(s.gitCmdFactory, req.Repository, s.cfg)

	// BEGIN TODO: Uncomment if StartPoint started behaving sensibly
	// like BranchName. See
	// https://gitlab.com/gitlab-org/gitaly/-/issues/3331
	//
	// startPointReference, err := localrepo.New(req.Repository).GetReference(ctx, "refs/heads/"+string(req.StartPoint))
	// startPointCommit, err := log.GetCommit(ctx, req.Repository, startPointReference.Target)
	startPointCommit, err := repo.ReadCommit(ctx, git.Revision(req.StartPoint))
	// END TODO
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "revspec '%s' not found", req.StartPoint)
	}

	referenceName := fmt.Sprintf("refs/heads/%s", req.BranchName)
	_, err = repo.GetReference(ctx, git.ReferenceName(referenceName))
	if err == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "Could not update %s. Please refresh and try again.", req.BranchName)
	} else if !errors.Is(err, git.ErrReferenceNotFound) {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if err := s.updateReferenceWithHooks(ctx, req.Repository, req.User, referenceName, startPointCommit.Id, git.ZeroOID.String()); err != nil {
		var preReceiveError preReceiveError
		if errors.As(err, &preReceiveError) {
			return &gitalypb.UserCreateBranchResponse{
				PreReceiveError: preReceiveError.message,
			}, nil
		}

		var updateRefError updateRefError
		if errors.As(err, &updateRefError) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}

		return nil, err
	}

	return &gitalypb.UserCreateBranchResponse{
		Branch: &gitalypb.Branch{
			Name:         req.BranchName,
			TargetCommit: startPointCommit,
		},
	}, nil
}

func (s *Server) UserUpdateBranch(ctx context.Context, req *gitalypb.UserUpdateBranchRequest) (*gitalypb.UserUpdateBranchResponse, error) {
	if featureflag.IsDisabled(ctx, featureflag.GoUserUpdateBranch) {
		return s.userUpdateBranchRuby(ctx, req)
	}
	return s.userUpdateBranchGo(ctx, req)
}

func validateUserUpdateBranchGo(req *gitalypb.UserUpdateBranchRequest) error {
	if req.User == nil {
		return status.Errorf(codes.InvalidArgument, "empty user")
	}

	if len(req.BranchName) == 0 {
		return status.Errorf(codes.InvalidArgument, "empty branch name")
	}

	if len(req.Oldrev) == 0 {
		return status.Errorf(codes.InvalidArgument, "empty oldrev")
	}

	if len(req.Newrev) == 0 {
		return status.Errorf(codes.InvalidArgument, "empty newrev")
	}

	return nil
}

func (s *Server) userUpdateBranchGo(ctx context.Context, req *gitalypb.UserUpdateBranchRequest) (*gitalypb.UserUpdateBranchResponse, error) {
	// Validate the request
	if err := validateUserUpdateBranchGo(req); err != nil {
		return nil, err
	}

	referenceName := fmt.Sprintf("refs/heads/%s", req.BranchName)
	if err := s.updateReferenceWithHooks(ctx, req.Repository, req.User, referenceName, string(req.Newrev), string(req.Oldrev)); err != nil {
		var preReceiveError preReceiveError
		if errors.As(err, &preReceiveError) {
			return &gitalypb.UserUpdateBranchResponse{
				PreReceiveError: preReceiveError.message,
			}, nil
		}

		// An oddball response for compatibility with the old
		// Ruby code. The "Could not update..."  message is
		// exactly like the default updateRefError, except we
		// say "branch-name", not
		// "refs/heads/branch-name". See the
		// "Gitlab::Git::CommitError" case in the Ruby code.
		return nil, status.Errorf(codes.FailedPrecondition, "Could not update %s. Please refresh and try again.", req.BranchName)
	}

	return &gitalypb.UserUpdateBranchResponse{}, nil
}

func (s *Server) userUpdateBranchRuby(ctx context.Context, req *gitalypb.UserUpdateBranchRequest) (*gitalypb.UserUpdateBranchResponse, error) {
	client, err := s.ruby.OperationServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, s.locator, req.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.UserUpdateBranch(clientCtx, req)
}

func (s *Server) UserDeleteBranch(ctx context.Context, req *gitalypb.UserDeleteBranchRequest) (*gitalypb.UserDeleteBranchResponse, error) {
	// That we do the branch name & user check here first only in
	// UserDelete but not UserCreate is "intentional", i.e. it's
	// always been that way.
	if len(req.BranchName) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Bad Request (empty branch name)")
	}

	if req.User == nil {
		return nil, status.Errorf(codes.InvalidArgument, "Bad Request (empty user)")
	}

	referenceName := fmt.Sprintf("refs/heads/%s", req.BranchName)

	referenceValue, err := localrepo.New(s.gitCmdFactory, req.Repository, s.cfg).GetReference(ctx, git.ReferenceName(referenceName))
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "branch not found: %s", req.BranchName)
	}

	if err := s.updateReferenceWithHooks(ctx, req.Repository, req.User, referenceName, git.ZeroOID.String(), referenceValue.Target); err != nil {
		var preReceiveError preReceiveError
		if errors.As(err, &preReceiveError) {
			return &gitalypb.UserDeleteBranchResponse{
				PreReceiveError: preReceiveError.message,
			}, nil
		}

		var updateRefError updateRefError
		if errors.As(err, &updateRefError) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}

		return nil, err
	}

	return &gitalypb.UserDeleteBranchResponse{}, nil
}
