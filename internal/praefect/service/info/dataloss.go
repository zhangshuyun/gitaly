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
	for _, repo := range repos {
		readOnly := true

		storages := make([]*gitalypb.DatalossCheckResponse_Repository_Storage, 0, len(repo.Storages))
		for _, storage := range repo.Storages {
			if storage.Name == repo.Primary && storage.BehindBy == 0 {
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
			RelativePath: repo.RelativePath,
			Primary:      repo.Primary,
			ReadOnly:     readOnly,
			Storages:     storages,
		})
	}

	return &gitalypb.DatalossCheckResponse{
		Repositories: pbRepos,
	}, nil
}
