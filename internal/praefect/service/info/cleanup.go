package server

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"golang.org/x/sync/errgroup"
)

func (s *Server) CleanupOrphanedRepos(ctx context.Context, in *gitalypb.CleanupOrphanedReposRequest) (*gitalypb.CleanupOrphanedReposResponse, error) {
	validRepos := make(map[string]struct{})

	for _, repo := range in.GetValidRepositories() {
		validRepos[repo.GetRelativePath()] = struct{}{}
	}

	allRepositories, err := s.datastore.GetRepositories()
	if err != nil {
		return nil, helper.ErrInternal(err)
	}

	// map keyed on storage with values of slices representing the repositories to remove for each primary storage
	repositoriesToRemove := make(map[string][]*models.Repository)

	for _, repositoryOnDisk := range allRepositories {
		if _, ok := validRepos[repositoryOnDisk.RelativePath]; !ok {
			repositoriesToRemove[repositoryOnDisk.Primary.Storage] = append(repositoriesToRemove[repositoryOnDisk.Primary.Storage], repositoryOnDisk)
		}
	}

	g, ctx := errgroup.WithContext(ctx)

	for storage, repos := range repositoriesToRemove {
		cc, ok := s.nodes[storage]
		if !ok {
			return nil, helper.ErrInternalf("could not find client connection for %s", storage)
		}

		repositoryClient := gitalypb.NewRepositoryServiceClient(cc)

		g.Go(func() error {
			for _, repo := range repos {
				if _, err = repositoryClient.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{
					Repository: &gitalypb.Repository{StorageName: repo.Primary.Storage, RelativePath: repo.RelativePath},
				}); err != nil {
					return err
				}
			}
			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.CleanupOrphanedReposResponse{}, nil
}
