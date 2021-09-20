package tempdir

import (
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
)

func TestCleanSuccess(t *testing.T) {
	cfg := testcfg.Build(t)
	locator := config.NewLocator(cfg)

	cleanRoot, err := locator.TempDir(cfg.Storages[0].Name)
	require.NoError(t, err)

	require.NoError(t, os.MkdirAll(cleanRoot, 0o755), "create clean root before setup")
	testhelper.MustRunCommand(t, nil, "chmod", "-R", "0700", cleanRoot)
	require.NoError(t, os.RemoveAll(cleanRoot), "clean up test clean root")

	old := time.Unix(0, 0)
	recent := time.Now()

	makeDir(t, locator, cfg.Storages[0], "a", old)
	makeDir(t, locator, cfg.Storages[0], "a/b", recent) // Messes up mtime of "a", we fix that below
	makeDir(t, locator, cfg.Storages[0], "c", recent)
	makeDir(t, locator, cfg.Storages[0], "f", old)

	makeFile(t, locator, cfg.Storages[0], "a/b/g", old)
	makeFile(t, locator, cfg.Storages[0], "c/d", old)
	makeFile(t, locator, cfg.Storages[0], "e", recent)

	// This is really evil and even breaks 'rm -rf'
	chmod(t, locator, cfg.Storages[0], "a/b", 0)
	chmod(t, locator, cfg.Storages[0], "a", 0)

	chtimes(t, locator, cfg.Storages[0], "a", old)

	assertEntries(t, locator, cfg.Storages[0], "a", "c", "e", "f")

	require.NoError(t, clean(locator, cfg.Storages[0]), "walk first pass")
	assertEntries(t, locator, cfg.Storages[0], "c", "e")
}

func TestCleanTempDir(t *testing.T) {
	cfg := testcfg.Build(t, testcfg.WithStorages("first", "second"))
	locator := config.NewLocator(cfg)

	gittest.CloneRepo(t, cfg, cfg.Storages[0])

	logrus.SetLevel(logrus.InfoLevel)
	logrus.SetOutput(io.Discard)

	hook := test.NewGlobal()

	cleanTempDir(locator, cfg.Storages)

	require.Equal(t, 2, len(hook.Entries), hook.Entries)
	require.Equal(t, "finished tempdir cleaner walk", hook.LastEntry().Message)
}

func TestCleanNoTmpExists(t *testing.T) {
	cfg := testcfg.Build(t)
	locator := config.NewLocator(cfg)

	require.NoError(t, clean(locator, cfg.Storages[0]))
}

func TestCleanNoStorageExists(t *testing.T) {
	cfg := testcfg.Build(t, testcfg.WithStorages("first"))
	locator := config.NewLocator(cfg)

	err := clean(locator, config.Storage{Name: "does-not-exist", Path: "/something"})
	require.EqualError(t, err, "temporary dir: rpc error: code = InvalidArgument desc = tmp dir: no such storage: \"does-not-exist\"")
}

type mockLocator struct {
	storage.Locator
}

func (m mockLocator) TempDir(storageName string) (string, error) {
	return "something", nil
}

func TestCleanerSafety(t *testing.T) {
	defer func() {
		if p := recover(); p != nil {
			if _, ok := p.(invalidCleanRoot); !ok {
				t.Fatalf("expected invalidCleanRoot panic, got %v", p)
			}
		}
	}()

	// We need to set up a mock locator which returns an invalid temporary directory path.
	require.NoError(t, clean(mockLocator{}, config.Storage{}))

	t.Fatal("expected panic")
}

func chmod(t *testing.T, locator storage.Locator, storage config.Storage, p string, mode os.FileMode) {
	root, err := locator.TempDir(storage.Name)
	require.NoError(t, err)
	require.NoError(t, os.Chmod(filepath.Join(root, p), mode))
}

func chtimes(t *testing.T, locator storage.Locator, storage config.Storage, p string, date time.Time) {
	root, err := locator.TempDir(storage.Name)
	require.NoError(t, err)
	require.NoError(t, os.Chtimes(filepath.Join(root, p), date, date))
}

func assertEntries(t *testing.T, locator storage.Locator, storage config.Storage, entries ...string) {
	root, err := locator.TempDir(storage.Name)
	require.NoError(t, err)

	foundEntries, err := os.ReadDir(root)
	require.NoError(t, err)

	require.Len(t, foundEntries, len(entries))

	for i, name := range entries {
		require.Equal(t, name, foundEntries[i].Name())
	}
}

func makeFile(t *testing.T, locator storage.Locator, storage config.Storage, filePath string, mtime time.Time) {
	root, err := locator.TempDir(storage.Name)
	require.NoError(t, err)

	fullPath := filepath.Join(root, filePath)
	require.NoError(t, os.WriteFile(fullPath, nil, 0o644))
	require.NoError(t, os.Chtimes(fullPath, mtime, mtime))
}

func makeDir(t *testing.T, locator storage.Locator, storage config.Storage, dirPath string, mtime time.Time) {
	root, err := locator.TempDir(storage.Name)
	require.NoError(t, err)

	fullPath := filepath.Join(root, dirPath)
	require.NoError(t, os.MkdirAll(fullPath, 0o700))
	require.NoError(t, os.Chtimes(fullPath, mtime, mtime))
}
