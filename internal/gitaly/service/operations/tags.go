package operations

import (
	"context"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) UserDeleteTag(ctx context.Context, req *gitalypb.UserDeleteTagRequest) (*gitalypb.UserDeleteTagResponse, error) {
	if featureflag.IsDisabled(ctx, featureflag.GoUserDeleteTag) {
		return s.UserDeleteTagRuby(ctx, req)
	}
	return s.UserDeleteTagGo(ctx, req)
}

func (s *Server) UserDeleteTagRuby(ctx context.Context, req *gitalypb.UserDeleteTagRequest) (*gitalypb.UserDeleteTagResponse, error) {
	client, err := s.ruby.OperationServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, s.locator, req.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.UserDeleteTag(clientCtx, req)
}

func (s *Server) UserDeleteTagGo(ctx context.Context, req *gitalypb.UserDeleteTagRequest) (*gitalypb.UserDeleteTagResponse, error) {
	if len(req.TagName) == 0 {
		// This one gets a new Internal error code. Then try
		// it with:
		//
		//     git checkout 495a384d4 -- internal/helper/error.go
		//
		// And it does not, but the message is mangled to:
		//
		//    rpc error: code = Internal desc = empty user
		return nil, helper.ErrInvalidArgument(status.Error(codes.Internal, "empty tag name"))
	}

	if req.User == nil {
		// This does not get a new error code, but the message
		// is mangled to:
		//
		//    rpc error: code = Internal desc = empty user
		//
		//     git checkout 495a384d4 -- internal/helper/error.go
		//
		// And the message is the same, but we mangle the
		// error code to InvalidArgument.
		return nil, helper.ErrInvalidArgumentf("%w", status.Error(codes.Internal, "empty user"))
	}

	referenceName := fmt.Sprintf("refs/tags/%s", req.TagName)
	revision, err := git.NewRepository(req.Repository).GetReference(ctx, referenceName)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "tag not found: %s", req.TagName)
	}

	if err := s.updateReferenceWithHooks(ctx, req.Repository, req.User, referenceName, git.NullSHA, revision.Target); err != nil {
		var preReceiveError preReceiveError
		if errors.As(err, &preReceiveError) {
			return &gitalypb.UserDeleteTagResponse{
				PreReceiveError: preReceiveError.message,
			}, nil
		}
		return nil, err
	}

	return &gitalypb.UserDeleteTagResponse{}, nil
}

func (s *Server) UserCreateTag(ctx context.Context, req *gitalypb.UserCreateTagRequest) (*gitalypb.UserCreateTagResponse, error) {
	client, err := s.ruby.OperationServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, s.locator, req.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.UserCreateTag(clientCtx, req)
}
