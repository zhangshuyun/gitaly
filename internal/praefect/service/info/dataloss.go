package info

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func (s *Server) DatalossCheck(ctx context.Context, req *gitalypb.DatalossCheckRequest) (*gitalypb.DatalossCheckResponse, error) {
	repos, err := s.rs.GetPartiallyAvailableRepositories(ctx, req.GetVirtualStorage())
	if err != nil {
		return nil, err
	}

	pbRepos := make([]*gitalypb.DatalossCheckResponse_Repository, 0, len(repos))
	for _, outdatedRepo := range repos {
		unavailable := true

		storages := make([]*gitalypb.DatalossCheckResponse_Repository_Storage, 0, len(outdatedRepo.Storages))
		for _, storage := range outdatedRepo.Storages {
			if storage.ValidPrimary {
				unavailable = false
			}

			storages = append(storages, &gitalypb.DatalossCheckResponse_Repository_Storage{
				Name:         storage.Name,
				BehindBy:     int64(storage.BehindBy),
				Assigned:     storage.Assigned,
				Healthy:      storage.Healthy,
				ValidPrimary: storage.ValidPrimary,
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
