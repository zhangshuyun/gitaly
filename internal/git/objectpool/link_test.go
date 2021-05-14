package objectpool

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestLink(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	pool, testRepo := setupObjectPool(t)

	require.NoError(t, pool.Remove(ctx), "make sure pool does not exist prior to creation")
	require.NoError(t, pool.Create(ctx, testRepo), "create pool")

	altPath, err := pool.locator.InfoAlternatesPath(testRepo)
	require.NoError(t, err)
	_, err = os.Stat(altPath)
	require.True(t, os.IsNotExist(err))

	require.NoError(t, pool.Link(ctx, testRepo))

	require.FileExists(t, altPath, "alternates file must exist after Link")

	content := testhelper.MustReadFile(t, altPath)
	require.True(t, strings.HasPrefix(string(content), "../"), "expected %q to be relative path", content)

	require.NoError(t, pool.Link(ctx, testRepo))

	newContent := testhelper.MustReadFile(t, altPath)
	require.Equal(t, content, newContent)

	require.False(t, gittest.RemoteExists(t, pool.cfg, pool.FullPath(), testRepo.GetGlRepository()), "pool remotes should not include %v", testRepo)
}

func TestLinkRemoveBitmap(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	pool, testRepo := setupObjectPool(t)
	require.NoError(t, pool.Init(ctx))

	testRepoPath := filepath.Join(pool.cfg.Storages[0].Path, testRepo.RelativePath)

	poolPath := pool.FullPath()
	gittest.Exec(t, pool.cfg, "-C", poolPath, "fetch", testRepoPath, "+refs/*:refs/*")

	gittest.Exec(t, pool.cfg, "-C", poolPath, "repack", "-adb")
	require.Len(t, listBitmaps(t, pool.FullPath()), 1, "pool bitmaps before")

	gittest.Exec(t, pool.cfg, "-C", testRepoPath, "repack", "-adb")
	require.Len(t, listBitmaps(t, testRepoPath), 1, "member bitmaps before")

	refsBefore := gittest.Exec(t, pool.cfg, "-C", testRepoPath, "for-each-ref")

	require.NoError(t, pool.Link(ctx, testRepo))

	require.Len(t, listBitmaps(t, pool.FullPath()), 1, "pool bitmaps after")
	require.Len(t, listBitmaps(t, testRepoPath), 0, "member bitmaps after")

	gittest.Exec(t, pool.cfg, "-C", testRepoPath, "fsck")

	refsAfter := gittest.Exec(t, pool.cfg, "-C", testRepoPath, "for-each-ref")
	require.Equal(t, refsBefore, refsAfter, "compare member refs before/after link")
}

func listBitmaps(t *testing.T, repoPath string) []string {
	entries, err := ioutil.ReadDir(filepath.Join(repoPath, "objects/pack"))
	require.NoError(t, err)

	var bitmaps []string
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".bitmap") {
			bitmaps = append(bitmaps, entry.Name())
		}
	}

	return bitmaps
}

func TestUnlink(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	pool, testRepo := setupObjectPool(t)

	require.Error(t, pool.Unlink(ctx, testRepo), "removing a non-existing pool should be an error")

	require.NoError(t, pool.Create(ctx, testRepo), "create pool")
	require.NoError(t, pool.Link(ctx, testRepo), "link test repo to pool")

	require.False(t, gittest.RemoteExists(t, pool.cfg, pool.FullPath(), testRepo.GetGlRepository()), "pool remotes should include %v", testRepo)

	require.NoError(t, pool.Unlink(ctx, testRepo), "unlink repo")
	require.False(t, gittest.RemoteExists(t, pool.cfg, pool.FullPath(), testRepo.GetGlRepository()), "pool remotes should no longer include %v", testRepo)
}

func TestLinkAbsoluteLinkExists(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	pool, testRepo := setupObjectPool(t)

	testRepoPath := filepath.Join(pool.cfg.Storages[0].Path, testRepo.RelativePath)

	require.NoError(t, pool.Remove(ctx), "make sure pool does not exist prior to creation")
	require.NoError(t, pool.Create(ctx, testRepo), "create pool")

	altPath, err := pool.locator.InfoAlternatesPath(testRepo)
	require.NoError(t, err)

	fullPath := filepath.Join(pool.FullPath(), "objects")

	require.NoError(t, ioutil.WriteFile(altPath, []byte(fullPath), 0644))

	require.NoError(t, pool.Link(ctx, testRepo), "we expect this call to change the absolute link to a relative link")

	require.FileExists(t, altPath, "alternates file must exist after Link")

	content := testhelper.MustReadFile(t, altPath)
	require.False(t, filepath.IsAbs(string(content)), "expected %q to be relative path", content)

	testRepoObjectsPath := filepath.Join(testRepoPath, "objects")
	require.Equal(t, fullPath, filepath.Join(testRepoObjectsPath, string(content)), "the content of the alternates file should be the relative version of the absolute pat")

	require.True(t, gittest.RemoteExists(t, pool.cfg, pool.FullPath(), "origin"), "pool remotes should include %v", testRepo)
}
