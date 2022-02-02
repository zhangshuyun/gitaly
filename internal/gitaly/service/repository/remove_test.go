package repository

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func TestRemoveRepository(t *testing.T) {
	t.Parallel()

	cfg, client := setupRepositoryServiceWithoutRepo(t)
	ctx := testhelper.Context(t)

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
	ctx := testhelper.Context(t)

	// Praefect special-cases repository removals, so we disable Praefect here.
	cfg, client := setupRepositoryServiceWithoutRepo(t, testserver.WithDisablePraefect())

	_, err := client.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{
		Repository: &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: "/does/not/exist"},
	})
	testhelper.RequireGrpcError(t, helper.ErrNotFoundf("repository does not exist"), err)
}

func TestRemoveRepository_locking(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	// Praefect special-cases repository removals, so we disable Praefect here.
	_, repo, repoPath, client := setupRepositoryService(ctx, t, testserver.WithDisablePraefect())

	// Simulate a concurrent RPC holding the repository lock.
	lockPath := repoPath + ".lock"
	require.NoError(t, os.WriteFile(lockPath, []byte{}, 0o644))
	defer func() { require.NoError(t, os.RemoveAll(lockPath)) }()

	_, err := client.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{Repository: repo})
	testhelper.RequireGrpcError(t, helper.ErrFailedPreconditionf("repository is already locked"), err)
}
