package objectpool

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"google.golang.org/grpc/peer"
)

func TestLink(t *testing.T) {
	ctx := testhelper.Context(t)

	cfg, pool, repoProto := setupObjectPool(t, ctx)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	require.NoError(t, pool.Remove(ctx), "make sure pool does not exist prior to creation")
	require.NoError(t, pool.Create(ctx, repo), "create pool")

	altPath, err := repo.InfoAlternatesPath()
	require.NoError(t, err)
	require.NoFileExists(t, altPath)

	require.NoError(t, pool.Link(ctx, repo))

	require.FileExists(t, altPath, "alternates file must exist after Link")

	content := testhelper.MustReadFile(t, altPath)
	require.True(t, strings.HasPrefix(string(content), "../"), "expected %q to be relative path", content)

	require.NoError(t, pool.Link(ctx, repo))

	newContent := testhelper.MustReadFile(t, altPath)
	require.Equal(t, content, newContent)

	require.False(t, gittest.RemoteExists(t, cfg, pool.FullPath(), repoProto.GetGlRepository()), "pool remotes should not include %v", repo)
}

func TestLink_transactional(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	cfg, pool, poolMemberProto := setupObjectPool(t, ctx)
	poolMember := localrepo.NewTestRepo(t, cfg, poolMemberProto)
	require.NoError(t, pool.Create(ctx, poolMember))

	txManager := transaction.NewTrackingManager()
	pool.txManager = txManager

	alternatesPath, err := poolMember.InfoAlternatesPath()
	require.NoError(t, err)
	require.NoFileExists(t, alternatesPath)

	ctx, err = txinfo.InjectTransaction(ctx, 1, "node", true)
	require.NoError(t, err)
	ctx = peer.NewContext(ctx, &peer.Peer{
		AuthInfo: backchannel.WithID(nil, 1234),
	})

	require.NoError(t, pool.Link(ctx, poolMember))

	require.Equal(t, 2, len(txManager.Votes()))
}

func TestLinkRemoveBitmap(t *testing.T) {
	ctx := testhelper.Context(t)

	cfg, pool, repoProto := setupObjectPool(t, ctx)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)
	require.NoError(t, pool.Init(ctx))

	testRepoPath := filepath.Join(cfg.Storages[0].Path, repo.GetRelativePath())

	poolPath := pool.FullPath()
	gittest.Exec(t, cfg, "-C", poolPath, "fetch", testRepoPath, "+refs/*:refs/*")

	gittest.Exec(t, cfg, "-C", poolPath, "repack", "-adb")
	require.Len(t, listBitmaps(t, pool.FullPath()), 1, "pool bitmaps before")

	gittest.Exec(t, cfg, "-C", testRepoPath, "repack", "-adb")
	require.Len(t, listBitmaps(t, testRepoPath), 1, "member bitmaps before")

	refsBefore := gittest.Exec(t, cfg, "-C", testRepoPath, "for-each-ref")

	require.NoError(t, pool.Link(ctx, repo))

	require.Len(t, listBitmaps(t, pool.FullPath()), 1, "pool bitmaps after")
	require.Len(t, listBitmaps(t, testRepoPath), 0, "member bitmaps after")

	gittest.Exec(t, cfg, "-C", testRepoPath, "fsck")

	refsAfter := gittest.Exec(t, cfg, "-C", testRepoPath, "for-each-ref")
	require.Equal(t, refsBefore, refsAfter, "compare member refs before/after link")
}

func listBitmaps(t *testing.T, repoPath string) []string {
	entries, err := os.ReadDir(filepath.Join(repoPath, "objects/pack"))
	require.NoError(t, err)

	var bitmaps []string
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".bitmap") {
			bitmaps = append(bitmaps, entry.Name())
		}
	}

	return bitmaps
}

func TestLinkAbsoluteLinkExists(t *testing.T) {
	ctx := testhelper.Context(t)

	cfg, pool, repoProto := setupObjectPool(t, ctx)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	testRepoPath := filepath.Join(cfg.Storages[0].Path, repo.GetRelativePath())

	require.NoError(t, pool.Remove(ctx), "make sure pool does not exist prior to creation")
	require.NoError(t, pool.Create(ctx, repo), "create pool")

	altPath, err := repo.InfoAlternatesPath()
	require.NoError(t, err)

	fullPath := filepath.Join(pool.FullPath(), "objects")

	require.NoError(t, os.WriteFile(altPath, []byte(fullPath), 0o644))

	require.NoError(t, pool.Link(ctx, repo), "we expect this call to change the absolute link to a relative link")

	require.FileExists(t, altPath, "alternates file must exist after Link")

	content := testhelper.MustReadFile(t, altPath)
	require.False(t, filepath.IsAbs(string(content)), "expected %q to be relative path", content)

	testRepoObjectsPath := filepath.Join(testRepoPath, "objects")
	require.Equal(t, fullPath, filepath.Join(testRepoObjectsPath, string(content)), "the content of the alternates file should be the relative version of the absolute pat")

	require.True(t, gittest.RemoteExists(t, cfg, pool.FullPath(), "origin"), "pool remotes should include %v", repo)
}
