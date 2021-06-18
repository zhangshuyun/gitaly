package repository

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func TestRemoveRepository(t *testing.T) {
	t.Parallel()
	_, repo, repoPath, client := setupRepositoryService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	_, err := client.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{Repository: repo})
	require.NoError(t, err)

	require.NoFileExists(t, repoPath)
}

func TestRemoveRepositoryDoesNotExist(t *testing.T) {
	t.Parallel()
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	_, err := client.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{
		Repository: &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: "/does/not/exist"}})
	require.NoError(t, err)
}
