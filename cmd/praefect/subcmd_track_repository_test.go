package main

import (
	"bytes"
	"flag"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/promtest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func TestAddRepository_FlagSet(t *testing.T) {
	t.Parallel()
	cmd := &trackRepository{}
	fs := cmd.FlagSet()
	require.NoError(t, fs.Parse([]string{"--virtual-storage", "vs", "--repository", "repo", "--authoritative-storage", "storage-0"}))
	require.Equal(t, "vs", cmd.virtualStorage)
	require.Equal(t, "repo", cmd.relativePath)
	require.Equal(t, "storage-0", cmd.authoritativeStorage)
}

func TestAddRepository_Exec_invalidArgs(t *testing.T) {
	t.Parallel()
	t.Run("not all flag values processed", func(t *testing.T) {
		cmd := trackRepository{}
		flagSet := flag.NewFlagSet("cmd", flag.PanicOnError)
		require.NoError(t, flagSet.Parse([]string{"stub"}))
		err := cmd.Exec(flagSet, config.Config{})
		require.EqualError(t, err, "cmd doesn't accept positional arguments")
	})

	t.Run("virtual-storage is not set", func(t *testing.T) {
		cmd := trackRepository{}
		err := cmd.Exec(flag.NewFlagSet("", flag.PanicOnError), config.Config{})
		require.EqualError(t, err, `"virtual-storage" is a required parameter`)
	})

	t.Run("repository is not set", func(t *testing.T) {
		cmd := trackRepository{virtualStorage: "stub"}
		err := cmd.Exec(flag.NewFlagSet("", flag.PanicOnError), config.Config{})
		require.EqualError(t, err, `"repository" is a required parameter`)
	})

	t.Run("authoritative-storage is not set", func(t *testing.T) {
		cmd := trackRepository{virtualStorage: "stub", relativePath: "path/to/repo"}
		err := cmd.Exec(flag.NewFlagSet("", flag.PanicOnError), config.Config{Failover: config.Failover{ElectionStrategy: config.ElectionStrategyPerRepository}})
		require.EqualError(t, err, `"authoritative-storage" is a required parameter`)
	})

	t.Run("db connection error", func(t *testing.T) {
		cmd := trackRepository{virtualStorage: "stub", relativePath: "stub", authoritativeStorage: "storage-0", logger: testhelper.NewDiscardingLogger(t)}
		cfg := config.Config{DB: config.DB{Host: "stub", SSLMode: "disable"}}
		err := cmd.Exec(flag.NewFlagSet("", flag.PanicOnError), cfg)
		require.Error(t, err)
		require.Contains(t, err.Error(), "connect to database: send ping: dial tcp: lookup stub")
	})
}

func TestAddRepository_Exec(t *testing.T) {
	t.Parallel()
	g1Cfg := testcfg.Build(t, testcfg.WithStorages("gitaly-1"))
	g2Cfg := testcfg.Build(t, testcfg.WithStorages("gitaly-2"))
	testcfg.BuildGitalyHooks(t, g2Cfg)
	testcfg.BuildGitalySSH(t, g2Cfg)

	g1Srv := testserver.StartGitalyServer(t, g1Cfg, nil, setup.RegisterAll, testserver.WithDisablePraefect())
	g2Srv := testserver.StartGitalyServer(t, g2Cfg, nil, setup.RegisterAll, testserver.WithDisablePraefect())
	defer g2Srv.Shutdown()
	defer g1Srv.Shutdown()

	g1Addr := g1Srv.Address()

	db := glsql.NewDB(t)
	dbConf := glsql.GetDBConfig(t, db.Name)

	virtualStorageName := "praefect"
	conf := config.Config{
		AllowLegacyElectors: true,
		SocketPath:          testhelper.GetTemporaryGitalySocketFileName(t),
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: virtualStorageName,
				Nodes: []*config.Node{
					{Storage: g1Cfg.Storages[0].Name, Address: g1Addr},
					{Storage: g2Cfg.Storages[0].Name, Address: g2Srv.Address()},
				},
				DefaultReplicationFactor: 2,
			},
		},
		DB: dbConf,
		Failover: config.Failover{
			Enabled:          true,
			ElectionStrategy: config.ElectionStrategyPerRepository,
		},
	}

	gitalyCC, err := client.Dial(g1Addr, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, gitalyCC.Close()) }()

	ctx, cancel := testhelper.Context()
	defer cancel()
	ctx = featureflag.OutgoingCtxWithFeatureFlag(ctx, featureflag.TxAtomicRepositoryCreation, true)

	gitaly1RepositoryClient := gitalypb.NewRepositoryServiceClient(gitalyCC)

	createRepoThroughGitaly1 := func(relativePath string) error {
		_, err := gitaly1RepositoryClient.CreateRepository(
			ctx,
			&gitalypb.CreateRepositoryRequest{
				Repository: &gitalypb.Repository{
					StorageName:  g1Cfg.Storages[0].Name,
					RelativePath: relativePath,
				},
			})
		return err
	}

	authoritativeStorage := g1Cfg.Storages[0].Name
	logger := testhelper.NewDiscardingLogger(t)

	t.Run("ok", func(t *testing.T) {
		testCases := []struct {
			relativePath         string
			desc                 string
			replicateImmediately bool
			expectedOutput       string
		}{
			{
				relativePath:         "path/to/test/repo1",
				desc:                 "force replication",
				replicateImmediately: true,
				expectedOutput:       "Finished replicating repository to \"gitaly-2\".\n",
			},
			{
				relativePath:         "path/to/test/repo2",
				desc:                 "do not force replication",
				replicateImmediately: false,
				expectedOutput:       "Added replication job to replicate repository to \"gitaly-2\".\n",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.desc, func(t *testing.T) {
				nodeMgr, err := nodes.NewManager(
					testhelper.NewDiscardingLogEntry(t),
					conf,
					db.DB,
					nil,
					promtest.NewMockHistogramVec(),
					protoregistry.GitalyProtoPreregistered,
					nil,
					nil,
					nil,
				)
				require.NoError(t, err)
				nodeMgr.Start(0, time.Hour)
				defer nodeMgr.Stop()

				repoDS := datastore.NewPostgresRepositoryStore(db, conf.StorageNames())

				rmRepoCmd := &removeRepository{
					logger:         logger,
					virtualStorage: virtualStorageName,
					relativePath:   tc.relativePath,
					w:              io.Discard,
					apply:          true,
				}
				require.NoError(t, rmRepoCmd.Exec(flag.NewFlagSet("", flag.PanicOnError), conf))

				// create the repo on Gitaly without Praefect knowing
				require.NoError(t, createRepoThroughGitaly1(tc.relativePath))
				require.DirExists(t, filepath.Join(g1Cfg.Storages[0].Path, tc.relativePath))
				require.NoDirExists(t, filepath.Join(g2Cfg.Storages[0].Path, tc.relativePath))
				var stdout bytes.Buffer

				addRepoCmd := &trackRepository{
					logger:               logger,
					virtualStorage:       virtualStorageName,
					relativePath:         tc.relativePath,
					authoritativeStorage: authoritativeStorage,
					replicateImmediately: tc.replicateImmediately,
					w:                    &stdout,
				}

				require.NoError(t, addRepoCmd.Exec(flag.NewFlagSet("", flag.PanicOnError), conf))
				as := datastore.NewAssignmentStore(db, conf.StorageNames())

				repositoryID, err := repoDS.GetRepositoryID(ctx, virtualStorageName, tc.relativePath)
				require.NoError(t, err)

				assignments, err := as.GetHostAssignments(ctx, virtualStorageName, repositoryID)
				require.NoError(t, err)
				require.Len(t, assignments, 2)
				assert.Contains(t, assignments, g1Cfg.Storages[0].Name)
				assert.Contains(t, assignments, g2Cfg.Storages[0].Name)

				exists, err := repoDS.RepositoryExists(ctx, virtualStorageName, tc.relativePath)
				require.NoError(t, err)
				assert.True(t, exists)
				assert.Contains(t, stdout.String(), tc.expectedOutput)

				if !tc.replicateImmediately {
					queue := datastore.NewPostgresReplicationEventQueue(db)
					events, err := queue.Dequeue(ctx, virtualStorageName, g2Cfg.Storages[0].Name, 1)
					require.NoError(t, err)
					assert.Len(t, events, 1)
					assert.Equal(t, tc.relativePath, events[0].Job.RelativePath)
				}
			})
		}
	})

	t.Run("repository does not exist", func(t *testing.T) {
		relativePath := "path/to/test/repo_1"

		cmd := &trackRepository{
			w:                    &bytes.Buffer{},
			logger:               testhelper.NewDiscardingLogger(t),
			virtualStorage:       "praefect",
			relativePath:         relativePath,
			authoritativeStorage: authoritativeStorage,
		}

		assert.ErrorIs(t, cmd.Exec(flag.NewFlagSet("", flag.PanicOnError), conf), errAuthoritativeRepositoryNotExist)
	})

	t.Run("records already exist", func(t *testing.T) {
		relativePath := "path/to/test/repo_2"

		require.NoError(t, createRepoThroughGitaly1(relativePath))
		require.DirExists(t, filepath.Join(g1Cfg.Storages[0].Path, relativePath))
		require.NoDirExists(t, filepath.Join(g2Cfg.Storages[0].Path, relativePath))

		ds := datastore.NewPostgresRepositoryStore(db, conf.StorageNames())
		id, err := ds.ReserveRepositoryID(ctx, virtualStorageName, relativePath)
		require.NoError(t, err)
		require.NoError(t, ds.CreateRepository(
			ctx,
			id,
			virtualStorageName,
			relativePath,
			relativePath,
			g1Cfg.Storages[0].Name,
			nil,
			nil,
			true,
			true,
		))

		cmd := &trackRepository{
			w:                    &bytes.Buffer{},
			logger:               testhelper.NewDiscardingLogger(t),
			virtualStorage:       virtualStorageName,
			relativePath:         relativePath,
			authoritativeStorage: authoritativeStorage,
		}

		assert.NoError(t, cmd.Exec(flag.NewFlagSet("", flag.PanicOnError), conf))
	})
}
