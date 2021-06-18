package repository

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testassert"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWriteCommitGraph_withExistingCommitGraphCreatedWithDefaults(t *testing.T) {
	t.Parallel()
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
	requireBloomFilterUsed(t, repoPath)
	require.NoFileExists(t, commitGraphPath, "commit-graph file should be replaced with commit graph chain")
}

func TestWriteCommitGraph_withExistingCommitGraphCreatedWithSplit(t *testing.T) {
	t.Parallel()
	cfg, repo, repoPath, client := setupRepositoryService(t)

	commitGraphPath := filepath.Join(repoPath, CommitGraphRelPath)
	require.NoFileExists(t, commitGraphPath, "sanity check no commit graph")

	chainPath := filepath.Join(repoPath, CommitGraphChainRelPath)
	require.NoFileExists(t, chainPath, "sanity check no commit graph chain exists")

	// write commit graph chain
	gittest.Exec(t, cfg, "-C", repoPath, "commit-graph", "write", "--reachable", "--split")
	require.FileExists(t, chainPath)

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
	requireBloomFilterUsed(t, repoPath)
	require.NoFileExists(t, commitGraphPath, "commit-graph file should not be created")
}

func TestWriteCommitGraph(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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

func requireBloomFilterUsed(t testing.TB, repoPath string) {
	t.Helper()

	commitGraphsPath := filepath.Join(repoPath, CommitGraphChainRelPath)
	ids := bytes.Split(testhelper.MustReadFile(t, commitGraphsPath), []byte{'\n'})

	for _, id := range ids {
		if len(id) == 0 {
			continue
		}
		graphFilePath := filepath.Join(repoPath, filepath.Dir(CommitGraphChainRelPath), fmt.Sprintf("graph-%s.graph", id))
		graphFileData := testhelper.MustReadFile(t, graphFilePath)

		require.True(t, bytes.HasPrefix(graphFileData, []byte("CGPH")), "4-byte signature of the commit graph file")
		require.True(t, bytes.Contains(graphFileData, []byte("BIDX")), "Bloom Filter Index")
		require.True(t, bytes.Contains(graphFileData, []byte("BDAT")), "Bloom Filter Data")
	}
}

func TestIsMissingBloomFilters(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		desc   string
		enable bool
	}{
		{desc: "no Bloom filter", enable: false},
		{desc: "with Bloom filter", enable: true},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			cfg := testcfg.Build(t)
			_, repoPath, cleanup := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], t.Name())
			t.Cleanup(cleanup)

			args := []string{"-C", repoPath, "commit-graph", "write", "--reachable", "--split"}
			if tc.enable {
				args = append(args, "--changed-paths")
			}
			gittest.Exec(t, cfg, args...)

			ok, err := isMissingBloomFilters(repoPath)
			require.NoError(t, err)
			require.Equal(t, tc.enable, !ok)
		})
	}
}
