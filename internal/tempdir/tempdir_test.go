package tempdir

import (
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
)

func TestNewRepositorySuccess(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	cfg := testcfg.Build(t)
	locator := config.NewLocator(cfg)

	repo, tempDir, err := NewRepository(ctx, cfg.Storages[0].Name, locator)
	require.NoError(t, err)
	require.Equal(t, cfg.Storages[0].Name, repo.StorageName)
	require.Contains(t, repo.RelativePath, tmpRootPrefix)

	calculatedPath, err := locator.GetPath(repo)
	require.NoError(t, err)
	require.Equal(t, tempDir.Path(), calculatedPath)

	require.NoError(t, ioutil.WriteFile(filepath.Join(tempDir.Path(), "test"), []byte("hello"), 0644))

	require.DirExists(t, tempDir.Path())

	cancel() // This should trigger async removal of the temporary directory
	tempDir.WaitForCleanup()

	require.NoDirExists(t, tempDir.Path())
}

func TestNewAsRepositoryFailStorageUnknown(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()
	_, err := New(ctx, "does-not-exist", config.NewLocator(config.Cfg{}))
	require.Error(t, err)
}
