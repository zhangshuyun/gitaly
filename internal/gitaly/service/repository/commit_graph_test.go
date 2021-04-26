package repository

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestWriteCommitGraph(t *testing.T) {
	cfg, repo, repoPath, client := setupRepositoryService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	commitGraphPath := filepath.Join(repoPath, CommitGraphRelPath)

	_, err := os.Stat(commitGraphPath)
	assert.True(t, os.IsNotExist(err))

	gittest.CreateCommit(
		t,
		cfg,
		repoPath,
		t.Name(),
		&gittest.CreateCommitOpts{Message: t.Name()},
	)

	res, err := client.WriteCommitGraph(ctx, &gitalypb.WriteCommitGraphRequest{Repository: repo})
	assert.NoError(t, err)
	assert.NotNil(t, res)

	assert.FileExists(t, commitGraphPath)
}

func TestUpdateCommitGraph(t *testing.T) {
	cfg, repo, repoPath, client := setupRepositoryService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	gittest.CreateCommit(
		t,
		cfg,
		repoPath,
		t.Name(),
		&gittest.CreateCommitOpts{Message: t.Name()},
	)

	commitGraphPath := filepath.Join(repoPath, CommitGraphRelPath)

	_, err := os.Stat(commitGraphPath)
	assert.True(t, os.IsNotExist(err))

	res, err := client.WriteCommitGraph(ctx, &gitalypb.WriteCommitGraphRequest{Repository: repo})
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.FileExists(t, commitGraphPath)

	// Reset the mtime of commit-graph file to use
	// as basis to detect changes
	assert.NoError(t, os.Chtimes(commitGraphPath, time.Time{}, time.Time{}))
	info, err := os.Stat(commitGraphPath)
	assert.NoError(t, err)
	mt := info.ModTime()

	gittest.CreateCommit(
		t,
		cfg,
		repoPath,
		t.Name(),
		&gittest.CreateCommitOpts{Message: t.Name()},
	)

	res, err = client.WriteCommitGraph(ctx, &gitalypb.WriteCommitGraphRequest{Repository: repo})
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.FileExists(t, commitGraphPath)

	assertModTimeAfter(t, mt, commitGraphPath)
}
