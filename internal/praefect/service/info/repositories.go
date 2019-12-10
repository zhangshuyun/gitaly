package info

import (
	"context"
	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"

	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"

	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// ListRepositories returns a list of repositories that includes the checksum of the primary as well as the replicas
func (s *Server) ListRepositories(in *gitalypb.ListRepositoriesRequest, stream gitalypb.InfoService_ListRepositoriesServer) error {
	repositories, err := s.datastore.GetRepositories()
	if err != nil {
		return helper.ErrInternal(err)
	}

	for _, repository := range repositories {
		if err = s.streamRepositoryDetails(stream, &repository); err != nil {
			return helper.ErrInternal(err)
		}
	}

	return nil
}

func (s *Server) streamRepositoryDetails(stream gitalypb.InfoService_ListRepositoriesServer, repository *models.Repository) error {
	var listRepositoriesResp gitalypb.ListRepositoriesResponse
	g, ctx := errgroup.WithContext(stream.Context())
	cc, err := s.clientCC.GetConnection(repository.Primary.Storage)
	if err != nil {
		return err
	}

	// primary
	g.Go(func() error {
		listRepositoriesResp.Primary, err = getRepositoryDetails(
			ctx,
			&gitalypb.Repository{
				StorageName:  repository.Primary.Storage,
				RelativePath: repository.RelativePath,
			}, cc)

		return err
	})

	// replicas
	listRepositoriesResp.Replicas = make([]*gitalypb.ListRepositoriesResponse_RepositoryDetails, len(repository.Replicas))

	for i, replica := range repository.Replicas {
		i := i             // rescoping
		replica := replica // rescoping
		cc, err := s.clientCC.GetConnection(replica.Storage)
		if err != nil {
			return err
		}

		g.Go(func() error {
			listRepositoriesResp.Replicas[i], err = getRepositoryDetails(ctx, &gitalypb.Repository{
				StorageName:  replica.Storage,
				RelativePath: repository.RelativePath,
			}, cc)

			return err
		})
	}

	if err := g.Wait(); err != nil {
		grpc_logrus.Extract(ctx).WithError(err).Error()
		return nil
	}

	if err := stream.Send(&listRepositoriesResp); err != nil {
		return err
	}

	return nil
}

func getRepositoryDetails(ctx context.Context, repo *gitalypb.Repository, cc *grpc.ClientConn) (*gitalypb.ListRepositoriesResponse_RepositoryDetails, error) {
	client := gitalypb.NewRepositoryServiceClient(cc)

	resp, err := client.CalculateChecksum(ctx,
		&gitalypb.CalculateChecksumRequest{
			Repository: repo,
		})
	if err != nil {
		return nil, err
	}

	return &gitalypb.ListRepositoriesResponse_RepositoryDetails{
		Repository: repo,
		Checksum:   resp.GetChecksum(),
	}, nil
}
