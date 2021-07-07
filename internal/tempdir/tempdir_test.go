package tempdir

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

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

	err = ioutil.WriteFile(filepath.Join(tempDir.Path(), "test"), []byte("hello"), 0644)
	require.NoError(t, err, "write file in tempdir")

	cancel() // This should trigger async removal of the temporary directory

	// Poll because the directory removal is async
	for i := 0; i < 100; i++ {
		_, err = os.Stat(tempDir.Path())
		if err != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	require.True(t, os.IsNotExist(err), "expected directory to have been removed, got error %v", err)
}

func TestNewAsRepositoryFailStorageUnknown(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()
	_, err := New(ctx, "does-not-exist", config.NewLocator(config.Cfg{}))
	require.Error(t, err)
}
