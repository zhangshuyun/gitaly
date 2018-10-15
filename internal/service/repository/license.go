package repository

import (
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"golang.org/x/net/context"
)

func (s *server) FindLicense(ctx context.Context, in *gitalypb.FindLicenseRequest) (*gitalypb.FindLicenseResponse, error) {
	client, err := s.RepositoryServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, in.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.FindLicense(clientCtx, in)
}
