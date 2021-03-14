package objectpool

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestDisconnectGitAlternates(t *testing.T) {
	cfg, repo, repoPath, locator, client, cleanup := setup(t)
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	gitCmdFactory := git.NewExecCommandFactory(cfg)
	pool, err := objectpool.NewObjectPool(cfg, locator, gitCmdFactory, repo.GetStorageName(), gittest.NewObjectPoolName(t))
	require.NoError(t, err)
	defer pool.Remove(ctx)

	require.NoError(t, pool.Create(ctx, repo))
	require.NoError(t, pool.Link(ctx, repo))
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "gc")

	existingObjectID := "55bc176024cfa3baaceb71db584c7e5df900ea65"

	// Corrupt the repository to check that existingObjectID can no longer be found
	altPath, err := locator.InfoAlternatesPath(repo)
	require.NoError(t, err, "find info/alternates")
	require.NoError(t, os.RemoveAll(altPath))

	cmd, err := gitCmdFactory.New(ctx, repo,
		git.SubCmd{Name: "cat-file", Flags: []git.Option{git.Flag{Name: "-e"}}, Args: []string{existingObjectID}})
	require.NoError(t, err)
	require.Error(t, cmd.Wait(), "expect cat-file to fail because object cannot be found")

	require.NoError(t, pool.Link(ctx, repo))
	require.FileExists(t, altPath, "objects/info/alternates should be back")

	// At this point we know that the repository has access to
	// existingObjectID, but only if objects/info/alternates is in place.

	_, err = client.DisconnectGitAlternates(ctx, &gitalypb.DisconnectGitAlternatesRequest{Repository: repo})
	require.NoError(t, err, "call DisconnectGitAlternates")

	// Check that the object can still be found, even though
	// objects/info/alternates is gone. This is the purpose of
	// DisconnectGitAlternates.
	testhelper.AssertPathNotExists(t, altPath)
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "cat-file", "-e", existingObjectID)
}

func TestDisconnectGitAlternatesNoAlternates(t *testing.T) {
	_, repo, repoPath, locator, client, cleanup := setup(t)
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	altPath, err := locator.InfoAlternatesPath(repo)
	require.NoError(t, err, "find info/alternates")
	testhelper.AssertPathNotExists(t, altPath)

	_, err = client.DisconnectGitAlternates(ctx, &gitalypb.DisconnectGitAlternatesRequest{Repository: repo})
	require.NoError(t, err, "call DisconnectGitAlternates on repository without alternates")

	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "fsck")
}

func TestDisconnectGitAlternatesUnexpectedAlternates(t *testing.T) {
	cfg, _, _, locator, client, cleanup := setup(t)
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testCases := []struct {
		desc       string
		altContent string
	}{
		{desc: "multiple alternates", altContent: "/foo/bar\n/qux/baz\n"},
		{desc: "directory not found", altContent: "/does/not/exist/\n"},
		{desc: "not a directory", altContent: "../HEAD\n"},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			repo, _, cleanupFn := gittest.CloneRepoAtStorage(t, cfg.Storages[0], t.Name())
			defer cleanupFn()

			altPath, err := locator.InfoAlternatesPath(repo)
			require.NoError(t, err, "find info/alternates")

			require.NoError(t, ioutil.WriteFile(altPath, []byte(tc.altContent), 0644))

			_, err = client.DisconnectGitAlternates(ctx, &gitalypb.DisconnectGitAlternatesRequest{Repository: repo})
			require.Error(t, err, "call DisconnectGitAlternates on repository with unexpected objects/info/alternates")

			contentAfterRPC, err := ioutil.ReadFile(altPath)
			require.NoError(t, err, "read back objects/info/alternates")
			require.Equal(t, tc.altContent, string(contentAfterRPC), "objects/info/alternates content should not have changed")
		})
	}
}

func TestRemoveAlternatesIfOk(t *testing.T) {
	cfg, repo, repoPath, locator, _, cleanup := setup(t)
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	altPath, err := locator.InfoAlternatesPath(repo)
	require.NoError(t, err, "find info/alternates")
	altContent := "/var/empty\n"
	require.NoError(t, ioutil.WriteFile(altPath, []byte(altContent), 0644), "write alternates file")

	// Intentionally break the repository, so that 'git fsck' will fail later.
	testhelper.MustRunCommand(t, nil, "sh", "-c", fmt.Sprintf("rm %s/objects/pack/*.pack", repoPath))

	altBackup := altPath + ".backup"

	srv := server{gitCmdFactory: git.NewExecCommandFactory(cfg)}
	err = srv.removeAlternatesIfOk(ctx, repo, altPath, altBackup)
	require.Error(t, err, "removeAlternatesIfOk should fail")
	require.IsType(t, &fsckError{}, err, "error must be because of fsck")

	// We expect objects/info/alternates to have been restored when
	// removeAlternatesIfOk returned.
	assertAlternates(t, altPath, altContent)

	// We expect the backup alternates file to still exist.
	assertAlternates(t, altBackup, altContent)
}

func assertAlternates(t *testing.T, altPath string, altContent string) {
	t.Helper()

	actualContent, err := ioutil.ReadFile(altPath)
	require.NoError(t, err, "read %s after fsck failure", altPath)

	require.Equal(t, altContent, string(actualContent), "%s content after fsck failure", altPath)
}
