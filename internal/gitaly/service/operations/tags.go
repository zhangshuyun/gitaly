package operations

import (
	"context"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) UserDeleteTag(ctx context.Context, req *gitalypb.UserDeleteTagRequest) (*gitalypb.UserDeleteTagResponse, error) {
	if featureflag.IsDisabled(ctx, featureflag.GoUserDeleteTag) {
		return s.UserDeleteTagRuby(ctx, req)
	}
	return s.UserDeleteTagGo(ctx, req)
}

func (s *server) UserDeleteTagRuby(ctx context.Context, req *gitalypb.UserDeleteTagRequest) (*gitalypb.UserDeleteTagResponse, error) {
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

func (s *server) UserDeleteTagGo(ctx context.Context, req *gitalypb.UserDeleteTagRequest) (*gitalypb.UserDeleteTagResponse, error) {
	if len(req.TagName) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Bad Request (empty tag name)")
	}

	if req.User == nil {
		return nil, status.Errorf(codes.InvalidArgument, "Bad Request (empty user)")
	}

	revision, err := git.NewRepository(req.Repository).GetReference(ctx, "refs/tags/"+string(req.TagName))
	if err != nil {
		return nil, helper.ErrPreconditionFailed(err)
	}

	tag := fmt.Sprintf("refs/tags/%s", req.TagName)

	if err := s.updateReferenceWithHooks(ctx, req.Repository, req.User, tag, git.NullSHA, revision.Name); err != nil {
		var preReceiveError preReceiveError
		if errors.As(err, &preReceiveError) {
			return &gitalypb.UserDeleteTagResponse{
				PreReceiveError: preReceiveError.message,
			}, nil
		}

		var updateRefError updateRefError
		if errors.As(err, &updateRefError) {
			// TODO: COpy/pasted from branches.go. See
			// "When an error happens[...]". Does it apply
			// here too?
			return &gitalypb.UserDeleteTagResponse{}, nil
		}

		return nil, err
	}

	return &gitalypb.UserDeleteTagResponse{}, nil
}

func (s *server) UserCreateTag(ctx context.Context, req *gitalypb.UserCreateTagRequest) (*gitalypb.UserCreateTagResponse, error) {
	if featureflag.IsDisabled(ctx, featureflag.GoUserCreateTag) {
		return s.UserCreateTagRuby(ctx, req)
	}
	// TODO: Implement GoUserCreateTag
	return s.UserCreateTagRuby(ctx, req)
}

func (s *server) UserCreateTagRuby(ctx context.Context, req *gitalypb.UserCreateTagRequest) (*gitalypb.UserCreateTagResponse, error) {
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
