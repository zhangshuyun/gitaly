package operations

import (
	"context"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) UserCreateBranch(ctx context.Context, req *gitalypb.UserCreateBranchRequest) (*gitalypb.UserCreateBranchResponse, error) {
	if featureflag.IsDisabled(ctx, featureflag.GoUserCreateBranch) {
		return s.UserCreateBranchRuby(ctx, req)
	}

	// Implement UserCreateBranch in Go

	if len(req.BranchName) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Bad Request (empty branch name)")
	}

	if req.User == nil {
		return nil, status.Errorf(codes.InvalidArgument, "Bad Request (empty user)")
	}

	if len(req.StartPoint) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Bad Request (empty starting point)")
	}

	startPointCommit, err := log.GetCommit(ctx, req.Repository, string(req.StartPoint))
	if err != nil {
		return nil, helper.ErrPreconditionFailed(err)
	}

	_, err = git.NewRepository(req.Repository, s.cfg).GetBranch(ctx, string(req.BranchName))
	if err == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "Bad Request (branch exists)")
	} else if !errors.Is(err, git.ErrReferenceNotFound) {
		return nil, status.Error(codes.Internal, err.Error())
	}

	branch := fmt.Sprintf("refs/heads/%s", req.BranchName)

	if err := s.updateReferenceWithHooks(ctx, req.Repository, req.User, branch, string(req.StartPoint), git.NullSHA); err != nil {
		var preReceiveError preReceiveError
		if errors.As(err, &preReceiveError) {
			return &gitalypb.UserCreateBranchResponse{
				PreReceiveError: preReceiveError.message,
			}, nil
		}

		var updateRefError updateRefError
		if errors.As(err, &updateRefError) {
			// When an error happens updating the reference, e.g. because of a race
			// with another update, then Ruby code didn't send an error but just an
			// empty response.
			return &gitalypb.UserCreateBranchResponse{}, nil
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

func (s *server) UserCreateBranchRuby(ctx context.Context, req *gitalypb.UserCreateBranchRequest) (*gitalypb.UserCreateBranchResponse, error) {
	client, err := s.ruby.OperationServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, s.locator, req.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.UserCreateBranch(clientCtx, req)
}

func (s *server) UserUpdateBranch(ctx context.Context, req *gitalypb.UserUpdateBranchRequest) (*gitalypb.UserUpdateBranchResponse, error) {
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

func (s *server) UserDeleteBranch(ctx context.Context, req *gitalypb.UserDeleteBranchRequest) (*gitalypb.UserDeleteBranchResponse, error) {
	if len(req.BranchName) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Bad Request (empty branch name)")
	}

	if req.User == nil {
		return nil, status.Errorf(codes.InvalidArgument, "Bad Request (empty user)")
	}

	if featureflag.IsDisabled(ctx, featureflag.GoUserDeleteBranch) {
		return s.UserDeleteBranchRuby(ctx, req)
	}

	// Implement UserDeleteBranch in Go

	revision, err := git.NewRepository(req.Repository, s.cfg).GetBranch(ctx, string(req.BranchName))
	if err != nil {
		return nil, helper.ErrPreconditionFailed(err)
	}

	branch := fmt.Sprintf("refs/heads/%s", req.BranchName)

	if err := s.updateReferenceWithHooks(ctx, req.Repository, req.User, branch, git.NullSHA, revision.Name); err != nil {
		var preReceiveError preReceiveError
		if errors.As(err, &preReceiveError) {
			return &gitalypb.UserDeleteBranchResponse{
				PreReceiveError: preReceiveError.message,
			}, nil
		}

		var updateRefError updateRefError
		if errors.As(err, &updateRefError) {
			// When an error happens updating the reference, e.g. because of a race
			// with another update, then Ruby code didn't send an error but just an
			// empty response.
			return &gitalypb.UserDeleteBranchResponse{}, nil
		}

		return nil, err
	}

	return &gitalypb.UserDeleteBranchResponse{}, nil
}

func (s *server) UserDeleteBranchRuby(ctx context.Context, req *gitalypb.UserDeleteBranchRequest) (*gitalypb.UserDeleteBranchResponse, error) {
	client, err := s.ruby.OperationServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, s.locator, req.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.UserDeleteBranch(clientCtx, req)
}
