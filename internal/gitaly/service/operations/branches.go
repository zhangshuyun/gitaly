package operations

import (
	"context"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v13/internal/git"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v13/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v13/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) UserCreateBranch(ctx context.Context, req *gitalypb.UserCreateBranchRequest) (*gitalypb.UserCreateBranchResponse, error) {
	client, err := s.ruby.OperationServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, req.GetRepository())
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

	clientCtx, err := rubyserver.SetHeaders(ctx, req.GetRepository())
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

	revision, err := parseRevision(ctx, req.Repository, string(req.BranchName))
	if err != nil {
		return nil, helper.ErrPreconditionFailed(err)
	}

	branch := fmt.Sprintf("refs/heads/%s", req.BranchName)

	if err := s.updateReferenceWithHooks(ctx, req.Repository, req.User, branch, git.NullSHA, revision); err != nil {
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

	clientCtx, err := rubyserver.SetHeaders(ctx, req.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.UserDeleteBranch(clientCtx, req)
}
