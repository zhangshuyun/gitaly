package objectpool

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestLink(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	pool, err := NewObjectPool(testRepo.GetStorageName(), testhelper.NewTestObjectPoolName(t))
	require.NoError(t, err)

	require.NoError(t, pool.Remove(ctx), "make sure pool does not exist prior to creation")
	require.NoError(t, pool.Create(ctx, testRepo), "create pool")

	altPath, err := git.AlternatesPath(testRepo)
	require.NoError(t, err)
	_, err = os.Stat(altPath)
	require.True(t, os.IsNotExist(err))

	require.NoError(t, pool.Link(ctx, testRepo))

	require.FileExists(t, altPath, "alternates file must exist after Link")

	content, err := ioutil.ReadFile(altPath)
	require.NoError(t, err)

	require.True(t, strings.HasPrefix(string(content), "../"), "expected %q to be relative path", content)

	require.NoError(t, pool.Link(ctx, testRepo))

	newContent, err := ioutil.ReadFile(altPath)
	require.NoError(t, err)

	require.Equal(t, content, newContent)

	// Test if the remote is set
	remotes := testhelper.MustRunCommand(t, nil, "git", "-C", pool.FullPath(), "remote")
	assert.Equal(t, testRepo.GetGlRepository()+"\n", string(remotes))
}

func TestUnlink(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	pool, err := NewObjectPool(testRepo.GetStorageName(), t.Name())
	require.NoError(t, err)
	defer pool.Remove(ctx)

	// Without a pool on disk, this doesn't return an error
	require.NoError(t, pool.Unlink(ctx, testRepo))

	altPath, err := git.AlternatesPath(testRepo)
	require.NoError(t, err)

	require.NoError(t, pool.Create(ctx, testRepo))
	require.NoError(t, pool.Link(ctx, testRepo))

	require.FileExists(t, altPath, "alternates file must exist after Link")

	require.NoError(t, pool.Unlink(ctx, testRepo))
	_, err = os.Stat(altPath)
	require.True(t, os.IsNotExist(err))
}
