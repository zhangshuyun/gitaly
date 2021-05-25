package info

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func (s *Server) DatalossCheck(ctx context.Context, req *gitalypb.DatalossCheckRequest) (*gitalypb.DatalossCheckResponse, error) {
	outdatedRepos, err := s.rs.GetPartiallyReplicatedRepositories(
		ctx, req.GetVirtualStorage(), s.conf.Failover.ElectionStrategy != config.ElectionStrategyPerRepository)
	if err != nil {
		return nil, err
	}

	pbRepos := make([]*gitalypb.DatalossCheckResponse_Repository, 0, len(outdatedRepos))
	for _, outdatedRepo := range outdatedRepos {
		readOnly := true

		storages := make([]*gitalypb.DatalossCheckResponse_Repository_Storage, 0, len(outdatedRepo.Storages))
		for _, storage := range outdatedRepo.Storages {
			if storage.Name == outdatedRepo.Primary && storage.BehindBy == 0 {
				readOnly = false
			}

			storages = append(storages, &gitalypb.DatalossCheckResponse_Repository_Storage{
				Name:     storage.Name,
				BehindBy: int64(storage.BehindBy),
				Assigned: storage.Assigned,
			})
		}

		if !req.IncludePartiallyReplicated && !readOnly {
			continue
		}

		pbRepos = append(pbRepos, &gitalypb.DatalossCheckResponse_Repository{
			RelativePath: outdatedRepo.RelativePath,
			Primary:      outdatedRepo.Primary,
			ReadOnly:     readOnly,
			Storages:     storages,
		})
	}

	return &gitalypb.DatalossCheckResponse{
		Repositories: pbRepos,
	}, nil
}
