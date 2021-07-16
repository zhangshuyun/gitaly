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
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func TestNewAsRepositorySuccess(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	cfg, repo, _ := testcfg.BuildWithRepo(t)
	locator := config.NewLocator(cfg)
	tempRepo, tempDir, err := NewAsRepository(ctx, repo, locator)
	require.NoError(t, err)
	require.NotEqual(t, repo, tempRepo)
	require.Equal(t, repo.StorageName, tempRepo.StorageName)
	require.NotEqual(t, repo.RelativePath, tempRepo.RelativePath)

	calculatedPath, err := locator.GetPath(tempRepo)
	require.NoError(t, err)
	require.Equal(t, tempDir, calculatedPath)

	err = ioutil.WriteFile(filepath.Join(tempDir, "test"), []byte("hello"), 0644)
	require.NoError(t, err, "write file in tempdir")

	cancel() // This should trigger async removal of the temporary directory

	// Poll because the directory removal is async
	for i := 0; i < 100; i++ {
		_, err = os.Stat(tempDir)
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
	_, err := New(ctx, &gitalypb.Repository{StorageName: "does-not-exist", RelativePath: "foobar.git"}, config.NewLocator(config.Cfg{}))
	require.Error(t, err)
}
