package repository

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestFsckSuccess(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(t, locator)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	c, err := client.Fsck(ctx, &gitalypb.FsckRequest{Repository: testRepo})
	assert.NoError(t, err)
	assert.NotNil(t, c)
	assert.Empty(t, c.GetError())
}

func TestFsckFailureSeverelyBrokenRepo(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(t, locator)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	// This makes the repo severely broken so that `git` does not identify it as a
	// proper repo.
	require.NoError(t, os.RemoveAll(filepath.Join(testRepoPath, "objects")))
	fd, err := os.Create(filepath.Join(testRepoPath, "objects"))
	require.NoError(t, err)
	require.NoError(t, fd.Close())

	c, err := client.Fsck(ctx, &gitalypb.FsckRequest{Repository: testRepo})
	assert.NoError(t, err)
	assert.NotNil(t, c)
	assert.Contains(t, strings.ToLower(string(c.GetError())), "not a git repository")
}

func TestFsckFailureSlightlyBrokenRepo(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(t, locator)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	// This makes the repo slightly broken so that `git` still identify it as a
	// proper repo, but `fsck` complains about broken refs...
	require.NoError(t, os.RemoveAll(filepath.Join(testRepoPath, "objects", "pack")))

	c, err := client.Fsck(ctx, &gitalypb.FsckRequest{Repository: testRepo})
	assert.NoError(t, err)
	assert.NotNil(t, c)
	assert.NotEmpty(t, string(c.GetError()))
	assert.Contains(t, string(c.GetError()), "error: HEAD: invalid sha1 pointer")
}
