package operations

import (
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"golang.org/x/net/context"
)

func (s *server) UserDeleteTag(ctx context.Context, req *gitalypb.UserDeleteTagRequest) (*gitalypb.UserDeleteTagResponse, error) {
	client, err := s.OperationServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, req.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.UserDeleteTag(clientCtx, req)
}

func (s *server) UserCreateTag(ctx context.Context, req *gitalypb.UserCreateTagRequest) (*gitalypb.UserCreateTagResponse, error) {
	client, err := s.OperationServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, req.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.UserCreateTag(clientCtx, req)
}
