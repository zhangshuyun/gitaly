package info

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

//nolint: revive,stylecheck // This is unintentionally missing documentation.
func (s *Server) DatalossCheck(ctx context.Context, req *gitalypb.DatalossCheckRequest) (*gitalypb.DatalossCheckResponse, error) {
	repos, err := s.rs.GetPartiallyAvailableRepositories(ctx, req.GetVirtualStorage())
	if err != nil {
		return nil, err
	}

	pbRepos := make([]*gitalypb.DatalossCheckResponse_Repository, 0, len(repos))
	for _, outdatedRepo := range repos {
		unavailable := true

		storages := make([]*gitalypb.DatalossCheckResponse_Repository_Storage, 0, len(outdatedRepo.Replicas))
		for _, replica := range outdatedRepo.Replicas {
			if replica.ValidPrimary {
				unavailable = false
			}

			storages = append(storages, &gitalypb.DatalossCheckResponse_Repository_Storage{
				Name:         replica.Storage,
				BehindBy:     outdatedRepo.Generation - replica.Generation,
				Assigned:     replica.Assigned,
				Healthy:      replica.Healthy,
				ValidPrimary: replica.ValidPrimary,
			})
		}

		if !req.IncludePartiallyReplicated && !unavailable {
			continue
		}

		pbRepos = append(pbRepos, &gitalypb.DatalossCheckResponse_Repository{
			RelativePath: outdatedRepo.RelativePath,
			Primary:      outdatedRepo.Primary,
			Unavailable:  unavailable,
			Storages:     storages,
		})
	}

	return &gitalypb.DatalossCheckResponse{
		Repositories: pbRepos,
	}, nil
}
