package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func (s *server) CleanupOrphanedRepos(ctx context.Context, in *gitalypb.CleanupOrphanedReposRequest) (*gitalypb.CleanupOrphanedReposResponse, error) {
	validRepos := make(map[string]struct{})

	for _, repo := range in.GetValidRepositories() {
		fullRepoPath, err := helper.GetRepoPath(repo)
		if err != nil {
			return nil, helper.ErrInternal(err)
		}
		validRepos[fullRepoPath] = struct{}{}
	}

	for _, storage := range config.Config.Storages {
		deleteDir, err := tempdir.ForDeletedRepositories(storage.Name)
		if err != nil {
			return nil, helper.ErrInternal(err)
		}

		filepath.Walk(storage.Path, func(path string, info os.FileInfo, err error) error {
			if !info.IsDir() || !strings.HasSuffix(path, ".git") {
				return nil
			}

			if _, ok := validRepos[path]; !ok {
				moveDir := filepath.Join(deleteDir, path)
				if err = os.MkdirAll(filepath.Dir(moveDir), 0755); err != nil {
					return err
				}

				if err = os.Rename(path, moveDir); err != nil {
					return err
				}
			}

			return nil
		})
	}

	return &gitalypb.CleanupOrphanedReposResponse{}, nil
}
