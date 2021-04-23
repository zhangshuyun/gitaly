package repository

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testassert"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWriteCommitGraph_withExistingCommitGraphCreatedWithDefaults(t *testing.T) {
	cfg, repo, repoPath, client := setupRepositoryService(t)

	commitGraphPath := filepath.Join(repoPath, CommitGraphRelPath)
	require.NoFileExists(t, commitGraphPath, "sanity check no commit graph")

	chainPath := filepath.Join(repoPath, CommitGraphChainRelPath)
	require.NoFileExists(t, chainPath, "sanity check no commit graph chain exists")

	// write commit graph using an old approach
	gittest.Exec(t, cfg, "-C", repoPath, "commit-graph", "write", "--reachable")
	require.FileExists(t, commitGraphPath)

	treeEntry := gittest.TreeEntry{Mode: "100644", Path: "file.txt", Content: "something"}
	gittest.WriteCommit(
		t,
		cfg,
		repoPath,
		gittest.WithBranch(t.Name()),
		gittest.WithTreeEntries(treeEntry),
	)

	ctx, cancel := testhelper.Context()
	defer cancel()

	res, err := client.WriteCommitGraph(ctx, &gitalypb.WriteCommitGraphRequest{
		Repository:    repo,
		SplitStrategy: gitalypb.WriteCommitGraphRequest_SizeMultiple,
	})
	require.NoError(t, err)
	require.NotNil(t, res)

	require.FileExists(t, chainPath, "commit graph chain should be created")
	require.NoFileExists(t, commitGraphPath, "commit-graph file should be replaced with commit graph chain")
}

func TestWriteCommitGraph(t *testing.T) {
	_, repo, repoPath, client := setupRepositoryService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	chainPath := filepath.Join(repoPath, CommitGraphChainRelPath)

	require.NoFileExists(t, chainPath)

	res, err := client.WriteCommitGraph(ctx, &gitalypb.WriteCommitGraphRequest{
		Repository:    repo,
		SplitStrategy: gitalypb.WriteCommitGraphRequest_SizeMultiple,
	})
	require.NoError(t, err)
	require.NotNil(t, res)

	require.FileExists(t, chainPath)
}

func TestWriteCommitGraph_validationChecks(t *testing.T) {
	_, repo, _, client := setupRepositoryService(t, testserver.WithDisablePraefect())

	ctx, cancel := testhelper.Context()
	defer cancel()

	for _, tc := range []struct {
		desc   string
		req    *gitalypb.WriteCommitGraphRequest
		expErr error
	}{
		{
			desc: "invalid split strategy",
			req: &gitalypb.WriteCommitGraphRequest{
				Repository:    repo,
				SplitStrategy: gitalypb.WriteCommitGraphRequest_SplitStrategy(42),
			},
			expErr: status.Error(codes.InvalidArgument, "unsupported split strategy: 42"),
		},
		{
			desc:   "no repository",
			req:    &gitalypb.WriteCommitGraphRequest{},
			expErr: status.Error(codes.InvalidArgument, `GetStorageByName: no such storage: ""`),
		},
		{
			desc:   "invalid storage",
			req:    &gitalypb.WriteCommitGraphRequest{Repository: &gitalypb.Repository{StorageName: "invalid"}},
			expErr: status.Error(codes.InvalidArgument, `GetStorageByName: no such storage: "invalid"`),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.WriteCommitGraph(ctx, tc.req)
			testassert.GrpcEqualErr(t, tc.expErr, err)
		})
	}
}

func TestUpdateCommitGraph(t *testing.T) {
	cfg, repo, repoPath, client := setupRepositoryService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	chainPath := filepath.Join(repoPath, CommitGraphChainRelPath)
	require.NoFileExists(t, chainPath)

	res, err := client.WriteCommitGraph(ctx, &gitalypb.WriteCommitGraphRequest{
		Repository:    repo,
		SplitStrategy: gitalypb.WriteCommitGraphRequest_SizeMultiple,
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.FileExists(t, chainPath)

	// Reset the mtime of commit-graph-chain file to use
	// as basis to detect changes
	require.NoError(t, os.Chtimes(chainPath, time.Time{}, time.Time{}))
	info, err := os.Stat(chainPath)
	require.NoError(t, err)
	mt := info.ModTime()

	treeEntry := gittest.TreeEntry{Mode: "100644", Path: "file.txt", Content: "something"}
	gittest.WriteCommit(
		t,
		cfg,
		repoPath,
		gittest.WithBranch(t.Name()),
		gittest.WithTreeEntries(treeEntry),
	)

	res, err = client.WriteCommitGraph(ctx, &gitalypb.WriteCommitGraphRequest{
		Repository:    repo,
		SplitStrategy: gitalypb.WriteCommitGraphRequest_SizeMultiple,
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.FileExists(t, chainPath)

	assertModTimeAfter(t, mt, chainPath)
}
