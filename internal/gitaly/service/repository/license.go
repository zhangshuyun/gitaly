package repository

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func (s *server) FindLicense(ctx context.Context, in *gitalypb.FindLicenseRequest) (*gitalypb.FindLicenseResponse, error) {
	client, err := s.ruby.RepositoryServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, s.locator, in.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.FindLicense(clientCtx, in)
}
