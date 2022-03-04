package info

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"golang.org/x/sync/errgroup"
)

// RepositoryReplicas returns a list of repositories that includes the checksum of the primary as well as the replicas
func (s *Server) RepositoryReplicas(ctx context.Context, in *gitalypb.RepositoryReplicasRequest) (*gitalypb.RepositoryReplicasResponse, error) {
	virtualStorage := in.GetRepository().GetStorageName()
	relativePath := in.GetRepository().GetRelativePath()

	repositoryID, err := s.rs.GetRepositoryID(ctx, virtualStorage, relativePath)
	if err != nil {
		return nil, fmt.Errorf("get repository id: %q", err)
	}

	primary, err := s.primaryGetter.GetPrimary(ctx, virtualStorage, repositoryID)
	if err != nil {
		return nil, fmt.Errorf("get primary: %w", err)
	}

	assignments, err := s.assignmentStore.GetHostAssignments(ctx, virtualStorage, repositoryID)
	if err != nil {
		return nil, fmt.Errorf("get host assignments: %w", err)
	}

	replicaPath := relativePath
	if s.conf.Failover.ElectionStrategy == config.ElectionStrategyPerRepository {
		replicaPath, err = s.rs.GetReplicaPath(ctx, repositoryID)
		if err != nil {
			return nil, fmt.Errorf("get replica path: %w", err)
		}
	}

	secondaries := make([]string, 0, len(assignments)-1)
	primaryIsAssigned := false
	for _, assignment := range assignments {
		if primary == assignment {
			primaryIsAssigned = true
			continue
		}

		secondaries = append(secondaries, assignment)
	}

	if !primaryIsAssigned {
		return nil, fmt.Errorf("primary %q is not an assigned host", primary)
	}

	var resp gitalypb.RepositoryReplicasResponse

	if resp.Primary, err = s.getRepositoryDetails(ctx, virtualStorage, primary, relativePath, replicaPath); err != nil {
		return nil, helper.ErrInternal(err)
	}

	resp.Replicas = make([]*gitalypb.RepositoryReplicasResponse_RepositoryDetails, len(secondaries))

	g, ctx := errgroup.WithContext(ctx)

	for i, storage := range secondaries {
		i := i             // rescoping
		storage := storage // rescoping
		g.Go(func() error {
			var err error
			resp.Replicas[i], err = s.getRepositoryDetails(ctx, virtualStorage, storage, relativePath, replicaPath)
			return err
		})
	}

	if err := g.Wait(); err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &resp, nil
}

func (s *Server) getRepositoryDetails(ctx context.Context, virtualStorage, storage, relativePath, replicaPath string) (*gitalypb.RepositoryReplicasResponse_RepositoryDetails, error) {
	conn, ok := s.conns[virtualStorage][storage]
	if !ok {
		return nil, fmt.Errorf("no connection to %q/%q", virtualStorage, storage)
	}

	resp, err := gitalypb.NewRepositoryServiceClient(conn).CalculateChecksum(ctx,
		&gitalypb.CalculateChecksumRequest{
			Repository: &gitalypb.Repository{
				StorageName:  storage,
				RelativePath: replicaPath,
			},
		})
	if err != nil {
		return nil, err
	}

	return &gitalypb.RepositoryReplicasResponse_RepositoryDetails{
		Repository: &gitalypb.Repository{
			StorageName:  storage,
			RelativePath: relativePath,
		},
		Checksum: resp.GetChecksum(),
	}, nil
}
