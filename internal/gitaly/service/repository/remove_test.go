package repository

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func TestRemoveRepository(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(
		featureflag.AtomicRemoveRepository,
		featureflag.TxAtomicRepositoryCreation,
	).Run(t, testRemoveRepository)
}

func testRemoveRepository(t *testing.T, ctx context.Context) {
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	repo := &gitalypb.Repository{
		StorageName:  cfg.Storages[0].Name,
		RelativePath: gittest.NewRepositoryName(t, true),
	}

	_, err := client.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{
		Repository: repo,
	})
	require.NoError(t, err)

	_, err = client.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{
		Repository: repo,
	})
	require.NoError(t, err)

	require.NoFileExists(t, filepath.Join(cfg.Storages[0].Path, repo.RelativePath))
}

func TestRemoveRepository_doesNotExist(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.AtomicRemoveRepository).Run(t, testRemoveRepositoryDoesNotExist)
}

func testRemoveRepositoryDoesNotExist(t *testing.T, ctx context.Context) {
	// Praefect special-cases repository removals, so we disable Praefect here.
	cfg, client := setupRepositoryServiceWithoutRepo(t, testserver.WithDisablePraefect())

	_, err := client.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{
		Repository: &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: "/does/not/exist"},
	})

	if featureflag.AtomicRemoveRepository.IsEnabled(ctx) {
		testhelper.RequireGrpcError(t, helper.ErrNotFoundf("repository does not exist"), err)
	} else {
		require.NoError(t, err)
	}
}

func TestRemoveRepository_locking(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.AtomicRemoveRepository).Run(t, testRemoveRepositoryLocking)
}

func testRemoveRepositoryLocking(t *testing.T, ctx context.Context) {
	// Praefect special-cases repository removals, so we disable Praefect here.
	_, repo, repoPath, client := setupRepositoryService(t, testserver.WithDisablePraefect())

	// Simulate a concurrent RPC holding the repository lock.
	require.NoError(t, os.WriteFile(repoPath+".lock", []byte{}, 0o644))

	_, err := client.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{Repository: repo})
	if featureflag.AtomicRemoveRepository.IsEnabled(ctx) {
		testhelper.RequireGrpcError(t, helper.ErrFailedPreconditionf("repository is already locked"), err)
	} else {
		require.NoError(t, err)
	}
}
