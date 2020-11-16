package operations

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func (s *server) UserDeleteTag(ctx context.Context, req *gitalypb.UserDeleteTagRequest) (*gitalypb.UserDeleteTagResponse, error) {
	if featureflag.IsDisabled(ctx, featureflag.GoUserDeleteTag) {
		return s.UserDeleteTagRuby(ctx, req)
	}
	// TODO: Implement GoUserDeleteTag
	return s.UserDeleteTagRuby(ctx, req)
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
