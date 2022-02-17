package repository

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestRepackIncrementalSuccess(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, repoPath, client := setupRepositoryService(ctx, t)

	packPath := filepath.Join(repoPath, "objects", "pack")

	// Reset mtime to a long while ago since some filesystems don't have sub-second
	// precision on `mtime`.
	// Stamp taken from https://golang.org/pkg/time/#pkg-constants
	testhelper.MustRunCommand(t, nil, "touch", "-t", testTimeString, filepath.Join(packPath, "*"))
	testTime := time.Date(2006, 0o1, 0o2, 15, 0o4, 0o5, 0, time.UTC)
	//nolint:staticcheck
	c, err := client.RepackIncremental(ctx, &gitalypb.RepackIncrementalRequest{Repository: repo})
	assert.NoError(t, err)
	assert.NotNil(t, c)

	// Entire `path`-folder gets updated so this is fine :D
	assertModTimeAfter(t, testTime, packPath)

	assert.FileExistsf(t,
		filepath.Join(repoPath, stats.CommitGraphChainRelPath),
		"pre-computed commit-graph should exist after running incremental repack",
	)
}

func TestRepackIncrementalCollectLogStatistics(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	logger, hook := test.NewNullLogger()
	_, repo, _, client := setupRepositoryService(ctx, t, testserver.WithLogger(logger))

	//nolint:staticcheck
	_, err := client.RepackIncremental(ctx, &gitalypb.RepackIncrementalRequest{Repository: repo})
	assert.NoError(t, err)

	mustCountObjectLog(t, hook.AllEntries()...)
}

func TestRepackLocal(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repo, repoPath, client := setupRepositoryService(ctx, t)

	altObjectsDir := "./alt-objects"
	alternateCommit := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithMessage("alternate commit"),
		gittest.WithAlternateObjectDirectory(filepath.Join(repoPath, altObjectsDir)),
		gittest.WithBranch("alternate-odb"),
	)

	repoCommit := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithMessage("main commit"),
		gittest.WithBranch("main-odb"),
	)

	// Set GIT_ALTERNATE_OBJECT_DIRECTORIES on the outgoing request. The
	// intended use case of the behavior we're testing here is that
	// alternates are found through the objects/info/alternates file instead
	// of GIT_ALTERNATE_OBJECT_DIRECTORIES. But for the purpose of this test
	// it doesn't matter.
	repo.GitAlternateObjectDirectories = []string{altObjectsDir}
	//nolint:staticcheck
	_, err := client.RepackFull(ctx, &gitalypb.RepackFullRequest{Repository: repo})
	require.NoError(t, err)

	packFiles, err := filepath.Glob(filepath.Join(repoPath, "objects", "pack", "pack-*.pack"))
	require.NoError(t, err)
	require.Len(t, packFiles, 1)

	packContents := gittest.Exec(t, cfg, "-C", repoPath, "verify-pack", "-v", packFiles[0])
	require.NotContains(t, string(packContents), alternateCommit.String())
	require.Contains(t, string(packContents), repoCommit.String())
}

func TestRepackIncrementalFailure(t *testing.T) {
	t.Parallel()
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	tests := []struct {
		repo *gitalypb.Repository
		code codes.Code
		desc string
	}{
		{desc: "nil repo", repo: nil, code: codes.InvalidArgument},
		{desc: "invalid storage name", repo: &gitalypb.Repository{StorageName: "foo"}, code: codes.InvalidArgument},
		{desc: "no storage name", repo: &gitalypb.Repository{RelativePath: "bar"}, code: codes.InvalidArgument},
		{desc: "non-existing repo", repo: &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: "bar"}, code: codes.NotFound},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)
			//nolint:staticcheck
			_, err := client.RepackIncremental(ctx, &gitalypb.RepackIncrementalRequest{Repository: test.repo})
			testhelper.RequireGrpcCode(t, err, test.code)
		})
	}
}

func TestRepackFullSuccess(t *testing.T) {
	t.Parallel()
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	tests := []struct {
		req  *gitalypb.RepackFullRequest
		desc string
	}{
		{req: &gitalypb.RepackFullRequest{CreateBitmap: true}, desc: "with bitmap"},
		{req: &gitalypb.RepackFullRequest{CreateBitmap: false}, desc: "without bitmap"},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)

			var repoPath string
			test.req.Repository, repoPath = gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
				Seed: gittest.SeedGitLabTest,
			})
			// Reset mtime to a long while ago since some filesystems don't have sub-second
			// precision on `mtime`.
			packPath := filepath.Join(repoPath, "objects", "pack")
			testhelper.MustRunCommand(t, nil, "touch", "-t", testTimeString, packPath)
			testTime := time.Date(2006, 0o1, 0o2, 15, 0o4, 0o5, 0, time.UTC)
			//nolint:staticcheck
			c, err := client.RepackFull(ctx, test.req)
			assert.NoError(t, err)
			assert.NotNil(t, c)

			// Entire `path`-folder gets updated so this is fine :D
			assertModTimeAfter(t, testTime, packPath)

			bmPath, err := filepath.Glob(filepath.Join(packPath, "pack-*.bitmap"))
			if err != nil {
				t.Fatalf("Error globbing bitmaps: %v", err)
			}
			if test.req.GetCreateBitmap() {
				if len(bmPath) == 0 {
					t.Errorf("No bitmaps found")
				}
				doBitmapsContainHashCache(t, bmPath)
			} else {
				if len(bmPath) != 0 {
					t.Errorf("Bitmap found: %v", bmPath)
				}
			}

			assert.FileExistsf(t,
				filepath.Join(repoPath, stats.CommitGraphChainRelPath),
				"pre-computed commit-graph should exist after running full repack",
			)
		})
	}
}

func TestRepackFullCollectLogStatistics(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	logger, hook := test.NewNullLogger()
	_, repo, _, client := setupRepositoryService(ctx, t, testserver.WithLogger(logger))

	//nolint:staticcheck
	_, err := client.RepackFull(ctx, &gitalypb.RepackFullRequest{Repository: repo})
	require.NoError(t, err)

	mustCountObjectLog(t, hook.AllEntries()...)
}

func mustCountObjectLog(t testing.TB, entries ...*logrus.Entry) {
	t.Helper()

	const key = "count_objects"
	for _, entry := range entries {
		if entry.Message == "git repo statistic" {
			require.Contains(t, entry.Data, "grpc.request.glProjectPath")
			require.Contains(t, entry.Data, "grpc.request.glRepository")
			require.Contains(t, entry.Data, key, "statistics not found")

			objectStats, ok := entry.Data[key].(map[string]interface{})
			require.True(t, ok, "expected count_objects to be a map")
			require.Contains(t, objectStats, "count")
			return
		}
	}
	require.FailNow(t, "no info about statistics")
}

func doBitmapsContainHashCache(t *testing.T, bitmapPaths []string) {
	// for each bitmap file, check the 2-byte flag as documented in
	// https://github.com/git/git/blob/master/Documentation/technical/bitmap-format.txt
	for _, bitmapPath := range bitmapPaths {
		gittest.TestBitmapHasHashcache(t, bitmapPath)
	}
}

func TestRepackFullFailure(t *testing.T) {
	t.Parallel()
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	tests := []struct {
		repo *gitalypb.Repository
		code codes.Code
		desc string
	}{
		{desc: "nil repo", repo: nil, code: codes.InvalidArgument},
		{desc: "invalid storage name", repo: &gitalypb.Repository{StorageName: "foo"}, code: codes.InvalidArgument},
		{desc: "no storage name", repo: &gitalypb.Repository{RelativePath: "bar"}, code: codes.InvalidArgument},
		{desc: "non-existing repo", repo: &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: "bar"}, code: codes.NotFound},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)
			//nolint:staticcheck
			_, err := client.RepackFull(ctx, &gitalypb.RepackFullRequest{Repository: test.repo})
			testhelper.RequireGrpcCode(t, err, test.code)
		})
	}
}

func TestRepackFullDeltaIslands(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repo, repoPath, client := setupRepositoryService(ctx, t)

	gittest.TestDeltaIslands(t, cfg, repoPath, func() error {
		//nolint:staticcheck
		_, err := client.RepackFull(ctx, &gitalypb.RepackFullRequest{Repository: repo})
		return err
	})
}

func TestLog2Threads(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		cpus    int
		threads string
	}{
		{1, "1"},
		{2, "1"},
		{3, "1"},
		{4, "2"},
		{8, "3"},
		{9, "3"},
		{13, "3"},
		{16, "4"},
		{27, "4"},
		{32, "5"},
	} {
		actualThreads := log2Threads(tt.cpus)
		require.Equal(t, tt.threads, actualThreads.Value)
	}
}
