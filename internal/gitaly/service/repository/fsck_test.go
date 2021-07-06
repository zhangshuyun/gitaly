package repository

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func TestFsckSuccess(t *testing.T) {
	t.Parallel()
	ctx, cancel := testhelper.Context()
	defer cancel()

	_, repo, _, client := setupRepositoryService(t)

	c, err := client.Fsck(ctx, &gitalypb.FsckRequest{Repository: repo})
	assert.NoError(t, err)
	assert.NotNil(t, c)
	assert.Empty(t, c.GetError())
}

func TestFsckFailureSeverelyBrokenRepo(t *testing.T) {
	t.Parallel()
	ctx, cancel := testhelper.Context()
	defer cancel()

	_, repo, repoPath, client := setupRepositoryService(t)

	// This makes the repo severely broken so that `git` does not identify it as a
	// proper repo.
	require.NoError(t, os.RemoveAll(filepath.Join(repoPath, "objects")))
	fd, err := os.Create(filepath.Join(repoPath, "objects"))
	require.NoError(t, err)
	require.NoError(t, fd.Close())

	c, err := client.Fsck(ctx, &gitalypb.FsckRequest{Repository: repo})
	assert.NoError(t, err)
	assert.NotNil(t, c)
	assert.Contains(t, strings.ToLower(string(c.GetError())), "not a git repository")
}

func TestFsckFailureSlightlyBrokenRepo(t *testing.T) {
	t.Parallel()
	ctx, cancel := testhelper.Context()
	defer cancel()

	_, repo, repoPath, client := setupRepositoryService(t)

	// This makes the repo slightly broken so that `git` still identify it as a
	// proper repo, but `fsck` complains about broken refs...
	require.NoError(t, os.RemoveAll(filepath.Join(repoPath, "objects", "pack")))

	c, err := client.Fsck(ctx, &gitalypb.FsckRequest{Repository: repo})
	assert.NoError(t, err)
	assert.NotNil(t, c)
	assert.NotEmpty(t, string(c.GetError()))
	assert.Contains(t, string(c.GetError()), "error: HEAD: invalid sha1 pointer")
}
