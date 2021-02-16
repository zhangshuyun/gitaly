package remote

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()
	cleanup := testhelper.Configure()
	defer cleanup()
	testhelper.ConfigureGitalySSH()
	return m.Run()
}

func TestRemoveRemote(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	gitCmdFactory := git.NewExecCommandFactory(config.Config)
	require.NoError(t, Remove(ctx, gitCmdFactory, config.Config, testRepo, "origin"))

	repoPath := filepath.Join(testhelper.GitlabTestStoragePath(), testRepo.RelativePath)

	out := testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "remote")
	require.Len(t, out, 0)
}

func TestRemoveRemoteDontRemoveLocalBranches(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	repoPath := filepath.Join(testhelper.GitlabTestStoragePath(), testRepo.RelativePath)

	//configure remote as fetch mirror
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "config", "remote.origin.fetch", "+refs/*:refs/*")
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "fetch")

	masterBeforeRemove := testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "show-ref", "refs/heads/master")

	gitCmdFactory := git.NewExecCommandFactory(config.Config)
	require.NoError(t, Remove(ctx, gitCmdFactory, config.Config, testRepo, "origin"))

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

	gitCmdFactory := git.NewExecCommandFactory(config.Config)

	found, err := Exists(ctx, gitCmdFactory, config.Config, testRepo, "origin")
	require.NoError(t, err)
	require.True(t, found)

	found, err = Exists(ctx, gitCmdFactory, config.Config, testRepo, "can-not-be-found")
	require.NoError(t, err)
	require.False(t, found)
}
