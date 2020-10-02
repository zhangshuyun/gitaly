package info

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v13/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// RepositoryReplicas returns a list of repositories that includes the checksum of the primary as well as the replicas
func (s *Server) RepositoryReplicas(ctx context.Context, in *gitalypb.RepositoryReplicasRequest) (*gitalypb.RepositoryReplicasResponse, error) {
	shard, err := s.nodeMgr.GetShard(in.GetRepository().GetStorageName())
	if err != nil {
		return nil, helper.ErrInternal(err)
	}

	var resp gitalypb.RepositoryReplicasResponse

	if resp.Primary, err = s.getRepositoryDetails(ctx, shard.Primary, in.GetRepository()); err != nil {
		return nil, helper.ErrInternal(err)
	}

	resp.Replicas = make([]*gitalypb.RepositoryReplicasResponse_RepositoryDetails, len(shard.Secondaries))

	g, ctx := errgroup.WithContext(ctx)

	for i, secondary := range shard.Secondaries {
		i := i                 // rescoping
		secondary := secondary // rescoping
		g.Go(func() error {
			var err error
			resp.Replicas[i], err = s.getRepositoryDetails(ctx, secondary, in.GetRepository())
			return err
		})
	}

	if err := g.Wait(); err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &resp, nil
}

func (s *Server) getRepositoryDetails(ctx context.Context, node nodes.Node, repository *gitalypb.Repository) (*gitalypb.RepositoryReplicasResponse_RepositoryDetails, error) {
	return getChecksum(
		ctx,
		&gitalypb.Repository{
			StorageName:  node.GetStorage(),
			RelativePath: repository.GetRelativePath(),
		}, node.GetConnection())
}

func getChecksum(ctx context.Context, repo *gitalypb.Repository, cc *grpc.ClientConn) (*gitalypb.RepositoryReplicasResponse_RepositoryDetails, error) {
	client := gitalypb.NewRepositoryServiceClient(cc)

	resp, err := client.CalculateChecksum(ctx,
		&gitalypb.CalculateChecksumRequest{
			Repository: repo,
		})
	if err != nil {
		return nil, err
	}

	return &gitalypb.RepositoryReplicasResponse_RepositoryDetails{
		Repository: repo,
		Checksum:   resp.GetChecksum(),
	}, nil
}
