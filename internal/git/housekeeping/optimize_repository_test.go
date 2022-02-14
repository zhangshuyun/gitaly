package housekeeping

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

type infiniteReader struct{}

func (r infiniteReader) Read(b []byte) (int, error) {
	for i := range b {
		b[i] = '\000'
	}
	return len(b), nil
}

func TestNeedsRepacking(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)

	for _, tc := range []struct {
		desc           string
		setup          func(t *testing.T) *gitalypb.Repository
		expectedErr    error
		expectedNeeded bool
		expectedConfig RepackObjectsConfig
	}{
		{
			desc: "empty repo",
			setup: func(t *testing.T) *gitalypb.Repository {
				repoProto, _ := gittest.InitRepo(t, cfg, cfg.Storages[0])
				return repoProto
			},
			// This is a bug: if the repo is empty then we wouldn't ever generate a
			// packfile, but we determine a repack is needed because it's missing a
			// bitmap. It's a rather benign bug though: git-repack(1) will exit
			// immediately because it knows that there's nothing to repack.
			expectedNeeded: true,
			expectedConfig: RepackObjectsConfig{
				FullRepack:  true,
				WriteBitmap: true,
			},
		},
		{
			desc: "missing bitmap",
			setup: func(t *testing.T) *gitalypb.Repository {
				repoProto, _ := gittest.CloneRepo(t, cfg, cfg.Storages[0])
				return repoProto
			},
			expectedNeeded: true,
			expectedConfig: RepackObjectsConfig{
				FullRepack:  true,
				WriteBitmap: true,
			},
		},
		{
			desc: "missing bitmap with alternate",
			setup: func(t *testing.T) *gitalypb.Repository {
				repoProto, repoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])

				// Create the alternates file. If it exists, then we shouldn't try
				// to generate a bitmap.
				require.NoError(t, os.WriteFile(filepath.Join(repoPath, "objects", "info", "alternates"), nil, 0o755))

				return repoProto
			},
			expectedNeeded: true,
			expectedConfig: RepackObjectsConfig{
				FullRepack:  true,
				WriteBitmap: false,
			},
		},
		{
			desc: "missing commit-graph",
			setup: func(t *testing.T) *gitalypb.Repository {
				repoProto, repoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])

				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-Ad", "--write-bitmap-index")

				return repoProto
			},
			expectedNeeded: true,
			expectedConfig: RepackObjectsConfig{
				FullRepack:  true,
				WriteBitmap: true,
			},
		},
		{
			desc: "commit-graph without bloom filters",
			setup: func(t *testing.T) *gitalypb.Repository {
				repoProto, repoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])

				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-Ad", "--write-bitmap-index")
				gittest.Exec(t, cfg, "-C", repoPath, "commit-graph", "write")

				return repoProto
			},
			expectedNeeded: true,
			expectedConfig: RepackObjectsConfig{
				FullRepack:  true,
				WriteBitmap: true,
			},
		},
		{
			desc: "no repack needed",
			setup: func(t *testing.T) *gitalypb.Repository {
				repoProto, repoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])

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
			repoProto, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])
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
			require.Equal(t, RepackObjectsConfig{
				FullRepack:  true,
				WriteBitmap: true,
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
			repoProto, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])
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
				require.Equal(t, RepackObjectsConfig{
					FullRepack:  false,
					WriteBitmap: false,
				}, repackCfg)
			}
		})
	}
}

func TestPruneIfNeeded(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

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

			repoProto, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0], gittest.InitRepoOpts{
				WithRelativePath: relativePath,
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

func TestPackRefsIfNeeded(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	const kiloByte = 1024

	for _, tc := range []struct {
		packedRefsSize int64
		requiredRefs   int
	}{
		{
			packedRefsSize: 1,
			requiredRefs:   16,
		},
		{
			packedRefsSize: 1 * kiloByte,
			requiredRefs:   16,
		},
		{
			packedRefsSize: 10 * kiloByte,
			requiredRefs:   33,
		},
		{
			packedRefsSize: 100 * kiloByte,
			requiredRefs:   49,
		},
		{
			packedRefsSize: 1000 * kiloByte,
			requiredRefs:   66,
		},
		{
			packedRefsSize: 10000 * kiloByte,
			requiredRefs:   82,
		},
		{
			packedRefsSize: 100000 * kiloByte,
			requiredRefs:   99,
		},
	} {
		t.Run(fmt.Sprintf("packed-refs with %d bytes", tc.packedRefsSize), func(t *testing.T) {
			repoProto, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			// Write an empty commit such that we can create valid refs.
			commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents())
			looseRefContent := []byte(commitID.String() + "\n")

			// We first create a single big packfile which is used to determine the
			// boundary of when we repack. We need to write a valid packed-refs file or
			// otherwise git-pack-refs(1) would choke later on, so we just write the
			// file such that every line is a separate ref of exactly 128 bytes in
			// length (a divisor of 1024), referring to the commit we created above.
			packedRefs, err := os.OpenFile(filepath.Join(repoPath, "packed-refs"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
			require.NoError(t, err)
			defer testhelper.MustClose(t, packedRefs)
			for i := int64(0); i < tc.packedRefsSize/128; i++ {
				packedRefLine := fmt.Sprintf("%s refs/something/this-line-is-padded-to-exactly-128-bytes-%030d\n", commitID.String(), i)
				require.Len(t, packedRefLine, 128)
				_, err := packedRefs.WriteString(packedRefLine)
				require.NoError(t, err)
			}
			require.NoError(t, packedRefs.Sync())

			// And then we create one less loose ref than we need to hit the boundary.
			// This is done to assert that we indeed don't repack before hitting the
			// boundary.
			for i := 0; i < tc.requiredRefs-1; i++ {
				looseRefPath := filepath.Join(repoPath, "refs", "heads", fmt.Sprintf("branch-%d", i))
				require.NoError(t, os.WriteFile(looseRefPath, looseRefContent, 0o644))
			}

			didRepack, err := packRefsIfNeeded(ctx, repo)
			require.NoError(t, err)
			require.False(t, didRepack)

			// Now we create the additional loose ref that causes us to hit the
			// boundary. We should thus see that we want to repack now.
			looseRefPath := filepath.Join(repoPath, "refs", "heads", "last-branch")
			require.NoError(t, os.WriteFile(looseRefPath, looseRefContent, 0o644))

			didRepack, err = packRefsIfNeeded(ctx, repo)
			require.NoError(t, err)
			require.True(t, didRepack)
		})
	}
}

func TestEstimateLooseObjectCount(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)
	repoProto, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])
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

func TestOptimizeRepository(t *testing.T) {
	cfg := testcfg.Build(t)
	txManager := transaction.NewManager(cfg, backchannel.NewRegistry())

	for _, tc := range []struct {
		desc                  string
		setup                 func(t *testing.T) *gitalypb.Repository
		expectedErr           error
		expectedOptimizations map[string]float64
	}{
		{
			desc: "empty repository tries to write bitmap",
			setup: func(t *testing.T) *gitalypb.Repository {
				repo, _ := gittest.InitRepo(t, cfg, cfg.Storages[0])
				return repo
			},
			expectedOptimizations: map[string]float64{
				"packed_objects": 1,
			},
		},
		{
			desc: "repository without bitmap repacks objects",
			setup: func(t *testing.T) *gitalypb.Repository {
				repo, _ := gittest.CloneRepo(t, cfg, cfg.Storages[0])
				return repo
			},
			expectedOptimizations: map[string]float64{
				"packed_objects": 1,
			},
		},
		{
			desc: "repository without commit-graph repacks objects",
			setup: func(t *testing.T) *gitalypb.Repository {
				repo, repoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])
				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-A", "--write-bitmap-index")
				return repo
			},
			expectedOptimizations: map[string]float64{
				"packed_objects": 1,
			},
		},
		{
			desc: "well-packed repository does not optimize",
			setup: func(t *testing.T) *gitalypb.Repository {
				repo, repoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])
				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-A", "--write-bitmap-index")
				gittest.Exec(t, cfg, "-C", repoPath, "commit-graph", "write", "--split", "--changed-paths")
				return repo
			},
		},
		{
			desc: "loose objects get pruned",
			setup: func(t *testing.T) *gitalypb.Repository {
				repo, repoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])
				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-A", "--write-bitmap-index")
				gittest.Exec(t, cfg, "-C", repoPath, "commit-graph", "write", "--split", "--changed-paths")

				// The repack won't repack the following objects because they're
				// broken, and thus we'll retry to prune them afterwards.
				require.NoError(t, os.MkdirAll(filepath.Join(repoPath, "objects", "17"), 0o755))
				for i := 0; i < 10; i++ {
					blobPath := filepath.Join(repoPath, "objects", "17", fmt.Sprintf("%d", i))
					require.NoError(t, os.WriteFile(blobPath, nil, 0o644))
				}

				return repo
			},
			expectedOptimizations: map[string]float64{
				"packed_objects": 1,
				"pruned_objects": 1,
			},
		},
		{
			desc: "loose refs get packed",
			setup: func(t *testing.T) *gitalypb.Repository {
				repo, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])

				for i := 0; i < 16; i++ {
					gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(), gittest.WithBranch(fmt.Sprintf("branch-%d", i)))
				}

				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-A", "--write-bitmap-index")
				gittest.Exec(t, cfg, "-C", repoPath, "commit-graph", "write", "--split", "--changed-paths")

				return repo
			},
			expectedOptimizations: map[string]float64{
				"packed_refs": 1,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)

			repoProto := tc.setup(t)
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			manager := NewManager(txManager)

			err := manager.OptimizeRepository(ctx, repo)
			require.Equal(t, tc.expectedErr, err)

			for _, metric := range []string{
				"packed_objects",
				"pruned_objects",
				"packed_refs",
			} {
				value := testutil.ToFloat64(manager.tasksTotal.WithLabelValues(metric))
				require.Equal(t, tc.expectedOptimizations[metric], value, metric)
			}
		})
	}
}
