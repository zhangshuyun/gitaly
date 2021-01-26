package objectpool

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestNewObjectPool(t *testing.T) {
	_, err := NewObjectPool(config.Config, config.NewLocator(config.Config), nil, "default", testhelper.NewTestObjectPoolName(t))
	require.NoError(t, err)

	_, err = NewObjectPool(config.Config, config.NewLocator(config.Config), nil, "mepmep", testhelper.NewTestObjectPoolName(t))
	require.Error(t, err, "creating pool in storage that does not exist should fail")
}

func TestNewFromRepoSuccess(t *testing.T) {
	ctx, cancel := testhelper.Context()
	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	relativePoolPath := testhelper.NewTestObjectPoolName(t)
	gitCmdFactory := git.NewExecCommandFactory(config.Config)

	pool, err := NewObjectPool(config.Config, config.NewLocator(config.Config), gitCmdFactory, testRepo.GetStorageName(), relativePoolPath)
	require.NoError(t, err)
	defer pool.Remove(ctx)

	defer cancel()
	require.NoError(t, pool.Create(ctx, testRepo))
	require.NoError(t, pool.Link(ctx, testRepo))

	poolFromRepo, err := FromRepo(config.Config, config.NewLocator(config.Config), gitCmdFactory, testRepo)
	require.NoError(t, err)
	require.Equal(t, relativePoolPath, poolFromRepo.relativePath)
	require.Equal(t, pool.storageName, poolFromRepo.storageName)
}

func TestNewFromRepoNoObjectPool(t *testing.T) {
	testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	gitCmdFactory := git.NewExecCommandFactory(config.Config)
	// no alternates file
	poolFromRepo, err := FromRepo(config.Config, config.NewLocator(config.Config), gitCmdFactory, testRepo)
	require.Equal(t, ErrAlternateObjectDirNotExist, err)
	require.Nil(t, poolFromRepo)

	// with an alternates file
	testCases := []struct {
		desc        string
		fileContent []byte
		expectedErr error
	}{
		{
			desc:        "points to non existent path",
			fileContent: []byte("/tmp/invalid_path"),
			expectedErr: ErrInvalidPoolRepository,
		},
		{
			desc:        "empty file",
			fileContent: nil,
			expectedErr: nil,
		},
		{
			desc:        "first line commented out",
			fileContent: []byte("#/tmp/invalid/path"),
			expectedErr: ErrAlternateObjectDirNotExist,
		},
	}

	require.NoError(t, os.MkdirAll(filepath.Join(testRepoPath, "objects", "info"), 0755))

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			alternateFilePath := filepath.Join(testRepoPath, "objects", "info", "alternates")
			require.NoError(t, ioutil.WriteFile(alternateFilePath, tc.fileContent, 0644))
			poolFromRepo, err := FromRepo(config.Config, config.NewLocator(config.Config), gitCmdFactory, testRepo)
			require.Equal(t, tc.expectedErr, err)
			require.Nil(t, poolFromRepo)

			require.NoError(t, os.Remove(alternateFilePath))
		})
	}
}

func TestCreate(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	masterSha := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "show-ref", "master")
	gitCmdFactory := git.NewExecCommandFactory(config.Config)
	pool, err := NewObjectPool(config.Config, config.NewLocator(config.Config), gitCmdFactory, testRepo.GetStorageName(), testhelper.NewTestObjectPoolName(t))
	require.NoError(t, err)

	err = pool.Create(ctx, testRepo)
	require.NoError(t, err)
	defer pool.Remove(ctx)

	require.True(t, pool.IsValid())

	// No hooks
	_, err = os.Stat(filepath.Join(pool.FullPath(), "hooks"))
	assert.True(t, os.IsNotExist(err))

	// origin is set
	out := testhelper.MustRunCommand(t, nil, "git", "-C", pool.FullPath(), "remote", "get-url", "origin")
	assert.Equal(t, testRepoPath, strings.TrimRight(string(out), "\n"))

	// refs exist
	out = testhelper.MustRunCommand(t, nil, "git", "-C", pool.FullPath(), "show-ref", "refs/heads/master")
	assert.Equal(t, masterSha, out)

	// No problems
	out = testhelper.MustRunCommand(t, nil, "git", "-C", pool.FullPath(), "cat-file", "-s", "55bc176024cfa3baaceb71db584c7e5df900ea65")
	assert.Equal(t, "282\n", string(out))
}

func TestCreateSubDirsExist(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	pool, err := NewObjectPool(config.Config, config.NewLocator(config.Config), git.NewExecCommandFactory(config.Config), testRepo.GetStorageName(), testhelper.NewTestObjectPoolName(t))
	defer pool.Remove(ctx)
	require.NoError(t, err)

	err = pool.Create(ctx, testRepo)
	require.NoError(t, err)

	pool.Remove(ctx)

	// Recreate pool so the subdirs exist already
	err = pool.Create(ctx, testRepo)
	require.NoError(t, err)
}

func TestRemove(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	pool, err := NewObjectPool(config.Config, config.NewLocator(config.Config), git.NewExecCommandFactory(config.Config), testRepo.GetStorageName(), testhelper.NewTestObjectPoolName(t))
	require.NoError(t, err)

	err = pool.Create(ctx, testRepo)
	require.NoError(t, err)

	require.True(t, pool.Exists())
	require.NoError(t, pool.Remove(ctx))
	require.False(t, pool.Exists())
}
