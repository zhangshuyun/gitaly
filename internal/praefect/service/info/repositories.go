package info

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// RepositoryReplicas returns a list of repositories that includes the checksum of the primary as well as the replicas
func (s *Server) RepositoryReplicas(ctx context.Context, in *gitalypb.RepositoryReplicasRequest) (*gitalypb.RepositoryReplicasResponse, error) {
	virtualStorage := in.GetRepository().GetStorageName()
	relativePath := in.GetRepository().GetRelativePath()

	primary, err := s.primaryGetter.GetPrimary(ctx, virtualStorage, relativePath)
	if err != nil {
		return nil, fmt.Errorf("get primary: %w", err)
	}

	assignments, err := s.assignmentStore.GetHostAssignments(ctx, virtualStorage, relativePath)
	if err != nil {
		return nil, fmt.Errorf("get host assignments: %w", err)
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

	if resp.Primary, err = s.getRepositoryDetails(ctx, virtualStorage, primary, relativePath); err != nil {
		return nil, helper.ErrInternal(err)
	}

	resp.Replicas = make([]*gitalypb.RepositoryReplicasResponse_RepositoryDetails, len(secondaries))

	g, ctx := errgroup.WithContext(ctx)

	for i, storage := range secondaries {
		i := i             // rescoping
		storage := storage // rescoping
		g.Go(func() error {
			var err error
			resp.Replicas[i], err = s.getRepositoryDetails(ctx, virtualStorage, storage, relativePath)
			return err
		})
	}

	if err := g.Wait(); err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &resp, nil
}

func (s *Server) getRepositoryDetails(ctx context.Context, virtualStorage, storage, relativePath string) (*gitalypb.RepositoryReplicasResponse_RepositoryDetails, error) {
	conn, ok := s.conns[virtualStorage][storage]
	if !ok {
		return nil, fmt.Errorf("no connection to %q/%q", virtualStorage, storage)
	}

	return getChecksum(
		ctx,
		&gitalypb.Repository{
			StorageName:  storage,
			RelativePath: relativePath,
		}, conn)
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
