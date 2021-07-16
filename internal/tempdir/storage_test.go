package tempdir

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
)

func TestCleanSuccess(t *testing.T) {
	require.NoError(t, os.MkdirAll(cleanRoot, 0755), "create clean root before setup")
	testhelper.MustRunCommand(t, nil, "chmod", "-R", "0700", cleanRoot)
	require.NoError(t, os.RemoveAll(cleanRoot), "clean up test clean root")

	old := time.Unix(0, 0)
	recent := time.Now()

	makeDir(t, "a", old)
	makeDir(t, "a/b", recent) // Messes up mtime of "a", we fix that below
	makeDir(t, "c", recent)
	makeDir(t, "f", old)

	makeFile(t, "a/b/g", old)
	makeFile(t, "c/d", old)
	makeFile(t, "e", recent)

	// This is really evil and even breaks 'rm -rf'
	require.NoError(t, chmod("a/b", 0), "apply evil permissions to 'a/b'")
	require.NoError(t, chmod("a", 0), "apply evil permissions to 'a'")

	require.NoError(t, chtimes("a", old), "reset mtime of 'a'")

	assertEntries(t, "a", "c", "e", "f")

	require.NoError(t, clean(cleanRoot), "walk first pass")
	assertEntries(t, "c", "e")
}

func TestCleanTempDir(t *testing.T) {
	cfg := testcfg.Build(t, testcfg.WithStorages("first", "second"))
	gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], t.Name())

	logrus.SetLevel(logrus.InfoLevel)
	logrus.SetOutput(ioutil.Discard)

	hook := test.NewGlobal()

	cleanTempDir(cfg.Storages)

	require.Equal(t, 2, len(hook.Entries), hook.Entries)
	require.Equal(t, "finished tempdir cleaner walk", hook.LastEntry().Message)
}

func TestCleanNoTmpExists(t *testing.T) {
	// This directory is valid because it ends in the special prefix
	dir := filepath.Join("testdata", "does-not-exist", tmpRootPrefix)

	require.NoError(t, clean(dir))
}

func TestCleanerSafety(t *testing.T) {
	defer func() {
		if p := recover(); p != nil {
			if _, ok := p.(invalidCleanRoot); !ok {
				t.Fatalf("expected invalidCleanRoot panic, got %v", p)
			}
		}
	}()

	//This directory is invalid because it does not end in '+gitaly/tmp'
	invalidDir := "testdata/does-not-exist"
	require.NoError(t, clean(invalidDir))

	t.Fatal("expected panic")
}

func chmod(p string, mode os.FileMode) error {
	return os.Chmod(filepath.Join(cleanRoot, p), mode)
}

func chtimes(p string, t time.Time) error {
	return os.Chtimes(filepath.Join(cleanRoot, p), t, t)
}

func assertEntries(t *testing.T, entries ...string) {
	foundEntries, err := ioutil.ReadDir(cleanRoot)
	require.NoError(t, err)

	require.Len(t, foundEntries, len(entries))

	for i, name := range entries {
		require.Equal(t, name, foundEntries[i].Name())
	}
}

func makeFile(t *testing.T, filePath string, mtime time.Time) {
	fullPath := filepath.Join(cleanRoot, filePath)
	require.NoError(t, ioutil.WriteFile(fullPath, nil, 0644))
	require.NoError(t, os.Chtimes(fullPath, mtime, mtime))
}

func makeDir(t *testing.T, dirPath string, mtime time.Time) {
	fullPath := filepath.Join(cleanRoot, dirPath)
	require.NoError(t, os.MkdirAll(fullPath, 0700))
	require.NoError(t, os.Chtimes(fullPath, mtime, mtime))
}
