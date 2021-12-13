package main

import (
	"bytes"
	"flag"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/bootstrap"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testdb"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func TestRemoveRepository_FlagSet(t *testing.T) {
	t.Parallel()
	cmd := &removeRepository{}
	fs := cmd.FlagSet()
	require.NoError(t, fs.Parse([]string{"--virtual-storage", "vs", "--repository", "repo"}))
	require.Equal(t, "vs", cmd.virtualStorage)
	require.Equal(t, "repo", cmd.relativePath)
}

func TestRemoveRepository_Exec_invalidArgs(t *testing.T) {
	t.Parallel()
	t.Run("not all flag values processed", func(t *testing.T) {
		cmd := removeRepository{}
		flagSet := flag.NewFlagSet("cmd", flag.PanicOnError)
		require.NoError(t, flagSet.Parse([]string{"stub"}))
		err := cmd.Exec(flagSet, config.Config{})
		require.EqualError(t, err, "cmd doesn't accept positional arguments")
	})

	t.Run("virtual-storage is not set", func(t *testing.T) {
		cmd := removeRepository{}
		err := cmd.Exec(flag.NewFlagSet("", flag.PanicOnError), config.Config{})
		require.EqualError(t, err, `"virtual-storage" is a required parameter`)
	})

	t.Run("repository is not set", func(t *testing.T) {
		cmd := removeRepository{virtualStorage: "stub"}
		err := cmd.Exec(flag.NewFlagSet("", flag.PanicOnError), config.Config{})
		require.EqualError(t, err, `"repository" is a required parameter`)
	})

	t.Run("db connection error", func(t *testing.T) {
		cmd := removeRepository{virtualStorage: "stub", relativePath: "stub"}
		cfg := config.Config{DB: config.DB{Host: "stub", SSLMode: "disable"}}
		err := cmd.Exec(flag.NewFlagSet("", flag.PanicOnError), cfg)
		require.Error(t, err)
		require.Contains(t, err.Error(), "connect to database: send ping: dial tcp: lookup stub")
	})
}

func TestRemoveRepository_Exec(t *testing.T) {
	t.Parallel()
	g1Cfg := testcfg.Build(t, testcfg.WithStorages("gitaly-1"))
	g2Cfg := testcfg.Build(t, testcfg.WithStorages("gitaly-2"))

	g1Addr := testserver.RunGitalyServer(t, g1Cfg, nil, setup.RegisterAll, testserver.WithDisablePraefect())
	g2Srv := testserver.StartGitalyServer(t, g2Cfg, nil, setup.RegisterAll, testserver.WithDisablePraefect())

	db := testdb.NewDB(t)
	dbConf := testdb.GetDBConfig(t, db.Name)

	conf := config.Config{
		SocketPath: testhelper.GetTemporaryGitalySocketFileName(t),
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "praefect",
				Nodes: []*config.Node{
					{Storage: g1Cfg.Storages[0].Name, Address: g1Addr},
					{Storage: g2Cfg.Storages[0].Name, Address: g2Srv.Address()},
				},
			},
		},
		DB: dbConf,
		Failover: config.Failover{
			Enabled:          true,
			ElectionStrategy: config.ElectionStrategyPerRepository,
		},
	}

	starterConfigs, err := getStarterConfigs(conf)
	require.NoError(t, err)
	stopped := make(chan struct{})
	bootstrapper := bootstrap.NewNoop()
	go func() {
		defer close(stopped)
		assert.NoError(t, run(starterConfigs, conf, bootstrapper, prometheus.NewRegistry(), prometheus.NewRegistry()))
	}()

	cc, err := client.Dial("unix://"+conf.SocketPath, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, cc.Close()) }()
	repoClient := gitalypb.NewRepositoryServiceClient(cc)

	ctx, cancel := testhelper.Context()
	defer cancel()

	praefectStorage := conf.VirtualStorages[0].Name

	t.Run("dry run", func(t *testing.T) {
		var out bytes.Buffer
		repo := createRepo(t, ctx, repoClient, praefectStorage, t.Name())
		cmd := &removeRepository{
			logger:         testhelper.NewDiscardingLogger(t),
			virtualStorage: repo.StorageName,
			relativePath:   repo.RelativePath,
			dialTimeout:    time.Second,
			apply:          false,
			w:              &writer{w: &out},
		}
		require.NoError(t, cmd.Exec(flag.NewFlagSet("", flag.PanicOnError), conf))

		require.DirExists(t, filepath.Join(g1Cfg.Storages[0].Path, repo.RelativePath))
		require.DirExists(t, filepath.Join(g2Cfg.Storages[0].Path, repo.RelativePath))
		assert.Contains(t, out.String(), "Repository found in the database.\n")
		assert.Contains(t, out.String(), "Repository found on the following gitaly nodes:")
		assert.Contains(t, out.String(), "Re-run the command with -apply to remove repositories from the database and disk.\n")

		repositoryRowExists, err := datastore.NewPostgresRepositoryStore(db, nil).RepositoryExists(ctx, cmd.virtualStorage, cmd.relativePath)
		require.NoError(t, err)
		require.True(t, repositoryRowExists)
	})

	t.Run("ok", func(t *testing.T) {
		var out bytes.Buffer
		repo := createRepo(t, ctx, repoClient, praefectStorage, t.Name())
		cmd := &removeRepository{
			logger:         testhelper.NewDiscardingLogger(t),
			virtualStorage: repo.StorageName,
			relativePath:   repo.RelativePath,
			dialTimeout:    time.Second,
			apply:          true,
			w:              &writer{w: &out},
		}
		require.NoError(t, cmd.Exec(flag.NewFlagSet("", flag.PanicOnError), conf))

		require.NoDirExists(t, filepath.Join(g1Cfg.Storages[0].Path, repo.RelativePath))
		require.NoDirExists(t, filepath.Join(g2Cfg.Storages[0].Path, repo.RelativePath))
		assert.Contains(t, out.String(), "Repository found in the database.\n")
		assert.Contains(t, out.String(), "Repository found on the following gitaly nodes:")
		assert.Contains(t, out.String(), fmt.Sprintf("Attempting to remove %s from the database, and delete it from all gitaly nodes...\n", repo.RelativePath))
		assert.Contains(t, out.String(), fmt.Sprintf("Successfully removed %s from", repo.RelativePath))

		var repositoryRowExists bool
		require.NoError(t, db.QueryRow(
			`SELECT EXISTS(SELECT FROM repositories WHERE virtual_storage = $1 AND relative_path = $2)`,
			cmd.virtualStorage, cmd.relativePath,
		).Scan(&repositoryRowExists))
		require.False(t, repositoryRowExists)
	})

	t.Run("no info about repository on praefect", func(t *testing.T) {
		var out bytes.Buffer
		repo := createRepo(t, ctx, repoClient, praefectStorage, t.Name())
		repoStore := datastore.NewPostgresRepositoryStore(db.DB, nil)
		_, _, err := repoStore.DeleteRepository(ctx, repo.StorageName, repo.RelativePath)
		require.NoError(t, err)

		cmd := &removeRepository{
			logger:         testhelper.NewDiscardingLogger(t),
			virtualStorage: praefectStorage,
			relativePath:   repo.RelativePath,
			dialTimeout:    time.Second,
			apply:          true,
			w:              &writer{w: &out},
		}
		require.NoError(t, cmd.Exec(flag.NewFlagSet("", flag.PanicOnError), conf))
		assert.Contains(t, out.String(), "Repository is not being tracked in Praefect.\n")
		assert.Contains(t, out.String(), "Repository found on the following gitaly nodes:")
		assert.Contains(t, out.String(), "The database has no information about this repository.\n")
		require.NoDirExists(t, filepath.Join(g1Cfg.Storages[0].Path, repo.RelativePath))
		require.NoDirExists(t, filepath.Join(g2Cfg.Storages[0].Path, repo.RelativePath))

		requireNoDatabaseInfo(t, db, cmd)
	})

	t.Run("one of gitalies is out of service", func(t *testing.T) {
		var out bytes.Buffer
		repo := createRepo(t, ctx, repoClient, praefectStorage, t.Name())
		g2Srv.Shutdown()

		logger := testhelper.NewDiscardingLogger(t)
		loggerHook := test.NewLocal(logger)
		cmd := &removeRepository{
			logger:         logrus.NewEntry(logger),
			virtualStorage: praefectStorage,
			relativePath:   repo.RelativePath,
			dialTimeout:    100 * time.Millisecond,
			apply:          true,
			w:              &writer{w: &out},
		}

		require.NoError(t, cmd.Exec(flag.NewFlagSet("", flag.PanicOnError), conf))

		var checkExistsOnNodeErrFound, removeRepoFromDiskErrFound bool
		for _, entry := range loggerHook.AllEntries() {
			if strings.Contains(entry.Message, `checking if repository exists on "gitaly-2"`) {
				checkExistsOnNodeErrFound = true
			}

			if strings.Contains(entry.Message, `repository removal failed for "gitaly-2"`) {
				removeRepoFromDiskErrFound = true
			}
		}
		require.True(t, checkExistsOnNodeErrFound)
		require.True(t, removeRepoFromDiskErrFound)

		require.NoDirExists(t, filepath.Join(g1Cfg.Storages[0].Path, repo.RelativePath))
		require.DirExists(t, filepath.Join(g2Cfg.Storages[0].Path, repo.RelativePath))

		requireNoDatabaseInfo(t, db, cmd)
	})

	bootstrapper.Terminate()
	<-stopped
}

func requireNoDatabaseInfo(t *testing.T, db testdb.DB, cmd *removeRepository) {
	t.Helper()
	var repositoryRowExists bool
	require.NoError(t, db.QueryRow(
		`SELECT EXISTS(SELECT FROM repositories WHERE virtual_storage = $1 AND relative_path = $2)`,
		cmd.virtualStorage, cmd.relativePath,
	).Scan(&repositoryRowExists))
	require.False(t, repositoryRowExists)
	var storageRowExists bool
	require.NoError(t, db.QueryRow(
		`SELECT EXISTS(SELECT FROM storage_repositories WHERE virtual_storage = $1 AND relative_path = $2)`,
		cmd.virtualStorage, cmd.relativePath,
	).Scan(&storageRowExists))
	require.False(t, storageRowExists)
}

func TestRemoveRepository_removeReplicationEvents(t *testing.T) {
	t.Parallel()
	const (
		virtualStorage = "praefect"
		relativePath   = "relative_path/to/repo.git"
	)

	ctx, cancel := testhelper.Context()
	defer cancel()

	db := testdb.NewDB(t)

	queue := datastore.NewPostgresReplicationEventQueue(db)

	// Set replication event in_progress.
	inProgressEvent, err := queue.Enqueue(ctx, datastore.ReplicationEvent{
		Job: datastore.ReplicationJob{
			Change:            datastore.CreateRepo,
			VirtualStorage:    virtualStorage,
			TargetNodeStorage: "gitaly-2",
			RelativePath:      relativePath,
		},
	})
	require.NoError(t, err)
	inProgress1, err := queue.Dequeue(ctx, virtualStorage, "gitaly-2", 10)
	require.NoError(t, err)
	require.Len(t, inProgress1, 1)

	// New event - events in the 'ready' state should be removed.
	_, err = queue.Enqueue(ctx, datastore.ReplicationEvent{
		Job: datastore.ReplicationJob{
			Change:            datastore.UpdateRepo,
			VirtualStorage:    virtualStorage,
			TargetNodeStorage: "gitaly-3",
			SourceNodeStorage: "gitaly-1",
			RelativePath:      relativePath,
		},
	})
	require.NoError(t, err)

	// Failed event - should be removed as well.
	failedEvent, err := queue.Enqueue(ctx, datastore.ReplicationEvent{
		Job: datastore.ReplicationJob{
			Change:            datastore.UpdateRepo,
			VirtualStorage:    virtualStorage,
			TargetNodeStorage: "gitaly-4",
			SourceNodeStorage: "gitaly-0",
			RelativePath:      relativePath,
		},
	})
	require.NoError(t, err)
	inProgress2, err := queue.Dequeue(ctx, virtualStorage, "gitaly-4", 10)
	require.NoError(t, err)
	require.Len(t, inProgress2, 1)
	// Acknowledge with failed status, so it will remain in the database for the next processing
	// attempt or until it is deleted by the 'removeReplicationEvents' method.
	acks2, err := queue.Acknowledge(ctx, datastore.JobStateFailed, []uint64{inProgress2[0].ID})
	require.NoError(t, err)
	require.Equal(t, []uint64{inProgress2[0].ID}, acks2)

	ticker := helper.NewManualTicker()
	defer ticker.Stop()

	errChan := make(chan error, 1)
	go func() {
		cmd := &removeRepository{virtualStorage: virtualStorage, relativePath: relativePath}
		errChan <- cmd.removeReplicationEvents(ctx, testhelper.NewDiscardingLogger(t), db.DB, ticker)
	}()

	ticker.Tick()
	ticker.Tick()
	ticker.Tick() // blocks until previous tick is consumed

	// Now we acknowledge in_progress job, so it stops the processing loop or the command.
	acks, err := queue.Acknowledge(ctx, datastore.JobStateCompleted, []uint64{inProgressEvent.ID})
	if assert.NoError(t, err) {
		assert.Equal(t, []uint64{inProgress1[0].ID}, acks)
	}

	ticker.Tick()

	timeout := time.After(time.Minute)
	for checkChan, exists := errChan, true; exists; {
		select {
		case err := <-checkChan:
			require.NoError(t, err)
			close(errChan)
			checkChan = nil
		case <-timeout:
			require.FailNow(t, "timeout reached, looks like the command hasn't made any progress")
		case <-time.After(50 * time.Millisecond):
			// Wait until job removed
			row := db.QueryRow(`SELECT EXISTS(SELECT FROM replication_queue WHERE id = $1)`, failedEvent.ID)
			require.NoError(t, row.Scan(&exists))
		}
	}
	// Once there are no in_progress jobs anymore the method returns.
	require.NoError(t, <-errChan)

	var notExists bool
	row := db.QueryRow(`SELECT NOT EXISTS(SELECT FROM replication_queue)`)
	require.NoError(t, row.Scan(&notExists))
	require.True(t, notExists)
}
