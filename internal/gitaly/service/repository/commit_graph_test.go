package repository

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
)

func TestWriteCommitGraph(t *testing.T) {
	s, stop := runRepoServer(t)
	defer stop()

	c, conn := newRepositoryClient(t, s)
	defer conn.Close()

	testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	commitGraphPath := filepath.Join(testRepoPath, CommitGraphRelPath)

	_, err := os.Stat(commitGraphPath)
	assert.True(t, os.IsNotExist(err))

	testhelper.CreateCommit(
		t,
		testRepoPath,
		t.Name(),
		&testhelper.CreateCommitOpts{Message: t.Name()},
	)

	res, err := c.WriteCommitGraph(ctx, &gitalypb.WriteCommitGraphRequest{Repository: testRepo})
	assert.NoError(t, err)
	assert.NotNil(t, res)

	assert.FileExists(t, commitGraphPath)
}

func TestUpdateCommitGraph(t *testing.T) {
	s, stop := runRepoServer(t)
	defer stop()

	c, conn := newRepositoryClient(t, s)
	defer conn.Close()

	testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testhelper.CreateCommit(
		t,
		testRepoPath,
		t.Name(),
		&testhelper.CreateCommitOpts{Message: t.Name()},
	)

	commitGraphPath := filepath.Join(testRepoPath, CommitGraphRelPath)

	_, err := os.Stat(commitGraphPath)
	assert.True(t, os.IsNotExist(err))

	res, err := c.WriteCommitGraph(ctx, &gitalypb.WriteCommitGraphRequest{Repository: testRepo})
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.FileExists(t, commitGraphPath)

	// Reset the mtime of commit-graph file to use
	// as basis to detect changes
	assert.NoError(t, os.Chtimes(commitGraphPath, time.Time{}, time.Time{}))
	info, err := os.Stat(commitGraphPath)
	assert.NoError(t, err)
	mt := info.ModTime()

	testhelper.CreateCommit(
		t,
		testRepoPath,
		t.Name(),
		&testhelper.CreateCommitOpts{Message: t.Name()},
	)

	res, err = c.WriteCommitGraph(ctx, &gitalypb.WriteCommitGraphRequest{Repository: testRepo})
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.FileExists(t, commitGraphPath)

	assertModTimeAfter(t, mt, commitGraphPath)
}
