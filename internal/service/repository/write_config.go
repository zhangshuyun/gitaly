package repository

import (
	"golang.org/x/net/context"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
)

func (s *server) WriteConfig(ctx context.Context, req *gitalypb.WriteConfigRequest) (*gitalypb.WriteConfigResponse, error) {
	client, err := s.RepositoryServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, req.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.WriteConfig(clientCtx, req)
}
