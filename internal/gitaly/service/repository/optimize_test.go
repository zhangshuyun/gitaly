package repository

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func getNewestPackfileModtime(t *testing.T, repoPath string) time.Time {
	t.Helper()

	packFiles, err := filepath.Glob(filepath.Join(repoPath, "objects", "pack", "*.pack"))
	require.NoError(t, err)
	if len(packFiles) == 0 {
		t.Error("no packfiles exist")
	}

	var newestPackfileModtime time.Time

	for _, packFile := range packFiles {
		info, err := os.Stat(packFile)
		require.NoError(t, err)
		if info.ModTime().After(newestPackfileModtime) {
			newestPackfileModtime = info.ModTime()
		}
	}

	return newestPackfileModtime
}

func TestOptimizeRepository(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repoProto, repoPath, client := setupRepositoryService(ctx, t)

	gittest.Exec(t, cfg, "-C", repoPath, "repack", "-A", "-b")
	gittest.Exec(t, cfg, "-C", repoPath, "commit-graph", "write", "--size-multiple=4", "--split=replace", "--reachable", "--changed-paths")

	hasBitmap, err := stats.HasBitmap(repoPath)
	require.NoError(t, err)
	require.True(t, hasBitmap, "expect a bitmap since we just repacked with -b")

	missingBloomFilters, err := stats.IsMissingBloomFilters(repoPath)
	require.NoError(t, err)
	require.False(t, missingBloomFilters)

	// get timestamp of latest packfile
	newestsPackfileTime := getNewestPackfileModtime(t, repoPath)

	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))

	gittest.Exec(t, cfg, "-C", repoPath, "config", "http.http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c1.git.extraHeader", "Authorization: Basic secret-password")
	gittest.Exec(t, cfg, "-C", repoPath, "config", "http.http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c2.git.extraHeader", "Authorization: Basic secret-password")
	gittest.Exec(t, cfg, "-C", repoPath, "config", "randomStart-http.http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c3.git.extraHeader", "Authorization: Basic secret-password")
	gittest.Exec(t, cfg, "-C", repoPath, "config", "http.http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c4.git.extraHeader-randomEnd", "Authorization: Basic secret-password")
	gittest.Exec(t, cfg, "-C", repoPath, "config", "hTTp.http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c5.git.ExtrAheaDeR", "Authorization: Basic secret-password")
	gittest.Exec(t, cfg, "-C", repoPath, "config", "http.http://extraHeader/extraheader/EXTRAHEADER.git.extraHeader", "Authorization: Basic secret-password")
	gittest.Exec(t, cfg, "-C", repoPath, "config", "https.https://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c5.git.extraHeader", "Authorization: Basic secret-password")
	confFileData := testhelper.MustReadFile(t, filepath.Join(repoPath, "config"))
	require.True(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c1.git")))
	require.True(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c2.git")))
	require.True(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c3")))
	require.True(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c4.git")))
	require.True(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c5.git")))
	require.True(t, bytes.Contains(confFileData, []byte("http://extraHeader/extraheader/EXTRAHEADER.git")))
	require.True(t, bytes.Contains(confFileData, []byte("https://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c5.git")))

	_, err = client.OptimizeRepository(ctx, &gitalypb.OptimizeRepositoryRequest{Repository: repoProto})
	require.NoError(t, err)

	confFileData = testhelper.MustReadFile(t, filepath.Join(repoPath, "config"))
	require.False(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c1.git")))
	require.False(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c2.git")))
	require.True(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c3")))
	require.True(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c4.git")))
	require.False(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c5.git")))
	require.False(t, bytes.Contains(confFileData, []byte("http://extraHeader/extraheader/EXTRAHEADER.git.extraHeader")))
	require.True(t, bytes.Contains(confFileData, []byte("https://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c5.git")))

	require.Equal(t, getNewestPackfileModtime(t, repoPath), newestsPackfileTime, "there should not have been a new packfile created")

	testRepoProto, testRepoPath := gittest.CreateRepository(ctx, t, cfg)

	blobs := 10
	blobIDs := gittest.WriteBlobs(t, cfg, testRepoPath, blobs)

	for _, blobID := range blobIDs {
		gittest.WriteCommit(t, cfg, testRepoPath,
			gittest.WithTreeEntries(gittest.TreeEntry{
				OID: git.ObjectID(blobID), Mode: "100644", Path: "blob",
			}),
			gittest.WithBranch(blobID),
			gittest.WithParents(),
		)
	}

	bitmaps, err := filepath.Glob(filepath.Join(testRepoPath, "objects", "pack", "*.bitmap"))
	require.NoError(t, err)
	require.Empty(t, bitmaps)

	mrRefs := filepath.Join(testRepoPath, "refs/merge-requests")
	emptyRef := filepath.Join(mrRefs, "1")
	require.NoError(t, os.MkdirAll(emptyRef, 0o755))
	require.DirExists(t, emptyRef, "sanity check for empty ref dir existence")

	// optimize repository on a repository without a bitmap should call repack full
	_, err = client.OptimizeRepository(ctx, &gitalypb.OptimizeRepositoryRequest{Repository: testRepoProto})
	require.NoError(t, err)

	bitmaps, err = filepath.Glob(filepath.Join(testRepoPath, "objects", "pack", "*.bitmap"))
	require.NoError(t, err)
	require.NotEmpty(t, bitmaps)

	missingBloomFilters, err = stats.IsMissingBloomFilters(testRepoPath)
	require.NoError(t, err)
	require.False(t, missingBloomFilters)

	// Empty directories should exist because they're too recent.
	require.DirExists(t, emptyRef)
	require.DirExists(t, mrRefs)
	require.FileExists(t,
		filepath.Join(testRepoPath, "refs/heads", blobIDs[0]),
		"unpacked refs should never be removed",
	)

	// Change the modification time to me older than a day and retry the call. Empty
	// directories must now be deleted.
	oneDayAgo := time.Now().Add(-24 * time.Hour)
	require.NoError(t, os.Chtimes(emptyRef, oneDayAgo, oneDayAgo))
	require.NoError(t, os.Chtimes(mrRefs, oneDayAgo, oneDayAgo))

	_, err = client.OptimizeRepository(ctx, &gitalypb.OptimizeRepositoryRequest{Repository: testRepoProto})
	require.NoError(t, err)

	require.NoFileExists(t, emptyRef)
	require.NoFileExists(t, mrRefs)
}

func TestOptimizeRepositoryValidation(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, _, client := setupRepositoryService(ctx, t)

	testCases := []struct {
		desc              string
		repo              *gitalypb.Repository
		expectedErrorCode codes.Code
	}{
		{
			desc:              "empty repository",
			repo:              nil,
			expectedErrorCode: codes.InvalidArgument,
		},
		{
			desc:              "invalid repository storage",
			repo:              &gitalypb.Repository{StorageName: "non-existent", RelativePath: repo.GetRelativePath()},
			expectedErrorCode: codes.InvalidArgument,
		},
		{
			desc:              "invalid repository path",
			repo:              &gitalypb.Repository{StorageName: repo.GetStorageName(), RelativePath: "/path/not/exist"},
			expectedErrorCode: codes.NotFound,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.OptimizeRepository(ctx, &gitalypb.OptimizeRepositoryRequest{Repository: tc.repo})
			require.Error(t, err)
			testhelper.RequireGrpcCode(t, err, tc.expectedErrorCode)
		})
	}

	_, err := client.OptimizeRepository(ctx, &gitalypb.OptimizeRepositoryRequest{Repository: repo})
	require.NoError(t, err)
}

type infiniteReader struct{}

func (r infiniteReader) Read(b []byte) (int, error) {
	for i := range b {
		b[i] = '\000'
	}
	return len(b), nil
}

func TestNeedsRepacking(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, _ := setupRepositoryServiceWithoutRepo(t)

	for _, tc := range []struct {
		desc           string
		setup          func(t *testing.T) *gitalypb.Repository
		expectedErr    error
		expectedNeeded bool
		expectedConfig repackCommandConfig
	}{
		{
			desc: "empty repo",
			setup: func(t *testing.T) *gitalypb.Repository {
				repoProto, _ := gittest.CreateRepository(ctx, t, cfg)
				return repoProto
			},
			// This is a bug: if the repo is empty then we wouldn't ever generate a
			// packfile, but we determine a repack is needed because it's missing a
			// bitmap. It's a rather benign bug though: git-repack(1) will exit
			// immediately because it knows that there's nothing to repack.
			expectedNeeded: true,
			expectedConfig: repackCommandConfig{
				fullRepack:  true,
				writeBitmap: true,
			},
		},
		{
			desc: "missing bitmap",
			setup: func(t *testing.T) *gitalypb.Repository {
				repoProto, _ := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
					Seed: gittest.SeedGitLabTest,
				})
				return repoProto
			},
			expectedNeeded: true,
			expectedConfig: repackCommandConfig{
				fullRepack:  true,
				writeBitmap: true,
			},
		},
		{
			desc: "missing bitmap with alternate",
			setup: func(t *testing.T) *gitalypb.Repository {
				repoProto, repoPath := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
					Seed: gittest.SeedGitLabTest,
				})

				// Create the alternates file. If it exists, then we shouldn't try
				// to generate a bitmap.
				require.NoError(t, os.WriteFile(filepath.Join(repoPath, "objects", "info", "alternates"), nil, 0o755))

				return repoProto
			},
			expectedNeeded: true,
			expectedConfig: repackCommandConfig{
				fullRepack:  true,
				writeBitmap: false,
			},
		},
		{
			desc: "missing commit-graph",
			setup: func(t *testing.T) *gitalypb.Repository {
				repoProto, repoPath := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
					Seed: gittest.SeedGitLabTest,
				})

				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-Ad", "--write-bitmap-index")

				return repoProto
			},
			expectedNeeded: true,
			expectedConfig: repackCommandConfig{
				fullRepack:  true,
				writeBitmap: true,
			},
		},
		{
			desc: "commit-graph without bloom filters",
			setup: func(t *testing.T) *gitalypb.Repository {
				repoProto, repoPath := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
					Seed: gittest.SeedGitLabTest,
				})

				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-Ad", "--write-bitmap-index")
				gittest.Exec(t, cfg, "-C", repoPath, "commit-graph", "write")

				return repoProto
			},
			expectedNeeded: true,
			expectedConfig: repackCommandConfig{
				fullRepack:  true,
				writeBitmap: true,
			},
		},
		{
			desc: "no repack needed",
			setup: func(t *testing.T) *gitalypb.Repository {
				repoProto, repoPath := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
					Seed: gittest.SeedGitLabTest,
				})

				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-Ad", "--write-bitmap-index")
				gittest.Exec(t, cfg, "-C", repoPath, "commit-graph", "write", "--changed-paths", "--split")

				return repoProto
			},
			expectedNeeded: false,
		},
	} {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			repoProto := tc.setup(t)
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			repackNeeded, repackCfg, err := needsRepacking(repo)
			require.Equal(t, tc.expectedErr, err)
			require.Equal(t, tc.expectedNeeded, repackNeeded)
			require.Equal(t, tc.expectedConfig, repackCfg)
		})
	}

	const megaByte = 1024 * 1024

	for _, tc := range []struct {
		packfileSize      int64
		requiredPackfiles int
	}{
		{
			packfileSize:      1,
			requiredPackfiles: 5,
		},
		{
			packfileSize:      5 * megaByte,
			requiredPackfiles: 6,
		},
		{
			packfileSize:      10 * megaByte,
			requiredPackfiles: 8,
		},
		{
			packfileSize:      50 * megaByte,
			requiredPackfiles: 14,
		},
		{
			packfileSize:      100 * megaByte,
			requiredPackfiles: 17,
		},
		{
			packfileSize:      500 * megaByte,
			requiredPackfiles: 23,
		},
		{
			packfileSize:      1000 * megaByte,
			requiredPackfiles: 26,
		},
		// Let's not go any further than this, we're thrashing the temporary directory.
	} {
		t.Run(fmt.Sprintf("packfile with %d bytes", tc.packfileSize), func(t *testing.T) {
			repoProto, repoPath := gittest.CreateRepository(ctx, t, cfg)
			repo := localrepo.NewTestRepo(t, cfg, repoProto)
			packDir := filepath.Join(repoPath, "objects", "pack")

			// Emulate the existence of a bitmap and a commit-graph with bloom filters.
			// We explicitly don't want to generate them via Git commands as they would
			// require us to already have objects in the repository, and we want to be
			// in full control over all objects and packfiles in the repo.
			require.NoError(t, os.WriteFile(filepath.Join(packDir, "something.bitmap"), nil, 0o644))
			commitGraphChainPath := filepath.Join(repoPath, stats.CommitGraphChainRelPath)
			require.NoError(t, os.MkdirAll(filepath.Dir(commitGraphChainPath), 0o755))
			require.NoError(t, os.WriteFile(commitGraphChainPath, nil, 0o644))

			// We first create a single big packfile which is used to determine the
			// boundary of when we repack.
			bigPackfile, err := os.OpenFile(filepath.Join(packDir, "big.pack"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
			require.NoError(t, err)
			defer testhelper.MustClose(t, bigPackfile)
			_, err = io.Copy(bigPackfile, io.LimitReader(infiniteReader{}, tc.packfileSize))
			require.NoError(t, err)

			// And then we create one less packfile than we need to hit the boundary.
			// This is done to assert that we indeed don't repack before hitting the
			// boundary.
			for i := 0; i < tc.requiredPackfiles-1; i++ {
				additionalPackfile, err := os.Create(filepath.Join(packDir, fmt.Sprintf("%d.pack", i)))
				require.NoError(t, err)
				testhelper.MustClose(t, additionalPackfile)
			}

			repackNeeded, _, err := needsRepacking(repo)
			require.NoError(t, err)
			require.False(t, repackNeeded)

			// Now we create the additional packfile that causes us to hit the boundary.
			// We should thus see that we want to repack now.
			lastPackfile, err := os.Create(filepath.Join(packDir, "last.pack"))
			require.NoError(t, err)
			testhelper.MustClose(t, lastPackfile)

			repackNeeded, repackCfg, err := needsRepacking(repo)
			require.NoError(t, err)
			require.True(t, repackNeeded)
			require.Equal(t, repackCommandConfig{
				fullRepack:  true,
				writeBitmap: true,
			}, repackCfg)
		})
	}

	for _, tc := range []struct {
		desc           string
		looseObjects   []string
		expectedRepack bool
	}{
		{
			desc:           "no objects",
			looseObjects:   nil,
			expectedRepack: false,
		},
		{
			desc: "object not in 17 shard",
			looseObjects: []string{
				filepath.Join("ab/12345"),
			},
			expectedRepack: false,
		},
		{
			desc: "object in 17 shard",
			looseObjects: []string{
				filepath.Join("17/12345"),
			},
			expectedRepack: false,
		},
		{
			desc: "objects in different shards",
			looseObjects: []string{
				filepath.Join("ab/12345"),
				filepath.Join("cd/12345"),
				filepath.Join("12/12345"),
				filepath.Join("17/12345"),
			},
			expectedRepack: false,
		},
		{
			desc: "boundary",
			looseObjects: []string{
				filepath.Join("17/1"),
				filepath.Join("17/2"),
				filepath.Join("17/3"),
				filepath.Join("17/4"),
			},
			expectedRepack: false,
		},
		{
			desc: "exceeding boundary should cause repack",
			looseObjects: []string{
				filepath.Join("17/1"),
				filepath.Join("17/2"),
				filepath.Join("17/3"),
				filepath.Join("17/4"),
				filepath.Join("17/5"),
			},
			expectedRepack: true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			repoProto, repoPath := gittest.CreateRepository(ctx, t, cfg)
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			// Emulate the existence of a bitmap and a commit-graph with bloom filters.
			// We explicitly don't want to generate them via Git commands as they would
			// require us to already have objects in the repository, and we want to be
			// in full control over all objects and packfiles in the repo.
			require.NoError(t, os.WriteFile(filepath.Join(repoPath, "objects", "pack", "something.bitmap"), nil, 0o644))
			commitGraphChainPath := filepath.Join(repoPath, stats.CommitGraphChainRelPath)
			require.NoError(t, os.MkdirAll(filepath.Dir(commitGraphChainPath), 0o755))
			require.NoError(t, os.WriteFile(commitGraphChainPath, nil, 0o644))

			for _, looseObjectPath := range tc.looseObjects {
				looseObjectPath := filepath.Join(repoPath, "objects", looseObjectPath)
				require.NoError(t, os.MkdirAll(filepath.Dir(looseObjectPath), 0o755))

				looseObjectFile, err := os.Create(looseObjectPath)
				require.NoError(t, err)
				testhelper.MustClose(t, looseObjectFile)
			}

			repackNeeded, repackCfg, err := needsRepacking(repo)
			require.NoError(t, err)
			require.Equal(t, tc.expectedRepack, repackNeeded)
			if tc.expectedRepack {
				require.Equal(t, repackCommandConfig{
					fullRepack:  false,
					writeBitmap: false,
				}, repackCfg)
			}
		})
	}
}

func TestPruneIfNeeded(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, _ := setupRepositoryServiceWithoutRepo(t)

	for _, tc := range []struct {
		desc          string
		isPool        bool
		looseObjects  []string
		expectedPrune bool
	}{
		{
			desc:          "no objects",
			looseObjects:  nil,
			expectedPrune: false,
		},
		{
			desc: "object not in 17 shard",
			looseObjects: []string{
				filepath.Join("ab/12345"),
			},
			expectedPrune: false,
		},
		{
			desc: "object in 17 shard",
			looseObjects: []string{
				filepath.Join("17/12345"),
			},
			expectedPrune: false,
		},
		{
			desc: "objects in different shards",
			looseObjects: []string{
				filepath.Join("ab/12345"),
				filepath.Join("cd/12345"),
				filepath.Join("12/12345"),
				filepath.Join("17/12345"),
			},
			expectedPrune: false,
		},
		{
			desc: "boundary",
			looseObjects: []string{
				filepath.Join("17/1"),
				filepath.Join("17/2"),
				filepath.Join("17/3"),
				filepath.Join("17/4"),
			},
			expectedPrune: false,
		},
		{
			desc: "exceeding boundary should cause repack",
			looseObjects: []string{
				filepath.Join("17/1"),
				filepath.Join("17/2"),
				filepath.Join("17/3"),
				filepath.Join("17/4"),
				filepath.Join("17/5"),
			},
			expectedPrune: true,
		},
		{
			desc:   "exceeding boundary on pool",
			isPool: true,
			looseObjects: []string{
				filepath.Join("17/1"),
				filepath.Join("17/2"),
				filepath.Join("17/3"),
				filepath.Join("17/4"),
				filepath.Join("17/5"),
			},
			expectedPrune: false,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			relativePath := gittest.NewRepositoryName(t, true)
			if tc.isPool {
				relativePath = gittest.NewObjectPoolName(t)
			}

			repoProto, repoPath := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
				RelativePath: relativePath,
			})
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			for _, looseObjectPath := range tc.looseObjects {
				looseObjectPath := filepath.Join(repoPath, "objects", looseObjectPath)
				require.NoError(t, os.MkdirAll(filepath.Dir(looseObjectPath), 0o755))

				looseObjectFile, err := os.Create(looseObjectPath)
				require.NoError(t, err)
				testhelper.MustClose(t, looseObjectFile)
			}

			didPrune, err := pruneIfNeeded(ctx, repo)
			require.Equal(t, tc.expectedPrune, didPrune)
			require.NoError(t, err)
		})
	}
}

func TestEstimateLooseObjectCount(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, _ := setupRepositoryServiceWithoutRepo(t)
	repoProto, repoPath := gittest.CreateRepository(ctx, t, cfg)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	t.Run("empty repository", func(t *testing.T) {
		looseObjects, err := estimateLooseObjectCount(repo)
		require.NoError(t, err)
		require.Zero(t, looseObjects)
	})

	t.Run("object in different shard", func(t *testing.T) {
		differentShard := filepath.Join(repoPath, "objects", "a0")
		require.NoError(t, os.MkdirAll(differentShard, 0o755))

		object, err := os.Create(filepath.Join(differentShard, "123456"))
		require.NoError(t, err)
		testhelper.MustClose(t, object)

		looseObjects, err := estimateLooseObjectCount(repo)
		require.NoError(t, err)
		require.Zero(t, looseObjects)
	})

	t.Run("object in estimation shard", func(t *testing.T) {
		estimationShard := filepath.Join(repoPath, "objects", "17")
		require.NoError(t, os.MkdirAll(estimationShard, 0o755))

		object, err := os.Create(filepath.Join(estimationShard, "123456"))
		require.NoError(t, err)
		testhelper.MustClose(t, object)

		looseObjects, err := estimateLooseObjectCount(repo)
		require.NoError(t, err)
		require.Equal(t, int64(256), looseObjects)

		// Create a second object in there.
		object, err = os.Create(filepath.Join(estimationShard, "654321"))
		require.NoError(t, err)
		testhelper.MustClose(t, object)

		looseObjects, err = estimateLooseObjectCount(repo)
		require.NoError(t, err)
		require.Equal(t, int64(512), looseObjects)
	})
}
