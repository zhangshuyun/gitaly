package housekeeping_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
)

func TestPruneIfNeeded(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, nil, setup.RegisterAll)

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
			t.Parallel()

			createRepoCfg := gittest.CreateRepositoryConfig{}
			if tc.isPool {
				createRepoCfg.RelativePath = gittest.NewObjectPoolName(t)
			}

			repoProto, repoPath := gittest.CreateRepository(ctx, t, cfg, createRepoCfg)
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			for _, looseObjectPath := range tc.looseObjects {
				looseObjectPath := filepath.Join(repoPath, "objects", looseObjectPath)
				require.NoError(t, os.MkdirAll(filepath.Dir(looseObjectPath), 0o755))

				looseObjectFile, err := os.Create(looseObjectPath)
				require.NoError(t, err)
				testhelper.MustClose(t, looseObjectFile)
			}

			logger, hook := test.NewNullLogger()
			ctx := ctxlogrus.ToContext(ctx, logrus.NewEntry(logger))

			require.NoError(t, housekeeping.NewManager(nil).OptimizeRepository(ctx, repo))
			require.Equal(t,
				struct {
					PackedObjects bool `json:"packed_objects"`
					PrunedObjects bool `json:"pruned_objects"`
					PackedRefs    bool `json:"packed_refs"`
				}{
					PackedObjects: true,
					PrunedObjects: tc.expectedPrune,
					PackedRefs:    false,
				},
				hook.Entries[len(hook.Entries)-1].Data["optimizations"],
			)
		})
	}
}
