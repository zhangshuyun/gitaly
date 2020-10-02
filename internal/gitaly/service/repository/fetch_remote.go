package repository

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
)

func (s *server) FetchRemote(ctx context.Context, in *gitalypb.FetchRemoteRequest) (*gitalypb.FetchRemoteResponse, error) {
	client, err := s.ruby.RepositoryServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, in.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.FetchRemote(clientCtx, in)
}
