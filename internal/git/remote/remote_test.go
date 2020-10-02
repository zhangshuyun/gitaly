package remote

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v13/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
)

func TestMain(m *testing.M) {
	testhelper.Configure()
	testhelper.ConfigureGitalySSH()
	os.Exit(m.Run())
}

func TestRemoveRemote(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	require.NoError(t, Remove(ctx, testRepo, "origin"))

	repoPath, err := helper.GetRepoPath(testRepo)
	require.NoError(t, err)

	out := testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "remote")
	require.Len(t, out, 0)
}

func TestRemoveRemoteDontRemoveLocalBranches(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	repoPath, err := helper.GetRepoPath(testRepo)
	require.NoError(t, err)

	//configure remote as fetch mirror
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "config", "remote.origin.fetch", "+refs/*:refs/*")
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "fetch")

	masterBeforeRemove := testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "show-ref", "refs/heads/master")

	require.NoError(t, Remove(ctx, testRepo, "origin"))

	out := testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "remote")
	require.Len(t, out, 0)

	out = testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "show-ref", "refs/heads/master")
	require.Equal(t, masterBeforeRemove, out)
}

func TestRemoteExists(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	found, err := Exists(ctx, testRepo, "origin")
	require.NoError(t, err)
	require.True(t, found)

	found, err = Exists(ctx, testRepo, "can-not-be-found")
	require.NoError(t, err)
	require.False(t, found)
}
