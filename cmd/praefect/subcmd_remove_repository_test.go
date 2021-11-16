package main

import (
	"flag"
	"os"
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
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func TestRemoveRepository_FlagSet(t *testing.T) {
	cmd := &removeRepository{}
	fs := cmd.FlagSet()
	require.NoError(t, fs.Parse([]string{"--virtual-storage", "vs", "--repository", "repo"}))
	require.Equal(t, "vs", cmd.virtualStorage)
	require.Equal(t, "repo", cmd.relativePath)
}

func TestRemoveRepository_Exec_invalidArgs(t *testing.T) {
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
		require.Contains(t, err.Error(), "connect to database: dial tcp: lookup stub")
	})

	t.Run("praefect address is not set in config ", func(t *testing.T) {
		cmd := removeRepository{virtualStorage: "stub", relativePath: "stub", logger: testhelper.NewTestLogger(t)}
		db := glsql.GetDB(t)
		dbConf := glsql.GetDBConfig(t, db.Name)
		err := cmd.Exec(flag.NewFlagSet("", flag.PanicOnError), config.Config{DB: dbConf})
		require.EqualError(t, err, "get praefect address from config: no Praefect address configured")
	})
}

func TestRemoveRepository_Exec(t *testing.T) {
	g1Cfg := testcfg.Build(t, testcfg.WithStorages("gitaly-1"))
	g2Cfg := testcfg.Build(t, testcfg.WithStorages("gitaly-2"))

	g1Addr := testserver.RunGitalyServer(t, g1Cfg, nil, setup.RegisterAll, testserver.WithDisablePraefect())
	g2Srv := testserver.StartGitalyServer(t, g2Cfg, nil, setup.RegisterAll, testserver.WithDisablePraefect())

	db := glsql.GetDB(t)
	dbConf := glsql.GetDBConfig(t, db.Name)

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
	bootstrapper := bootstrap.NewNoop()
	go func() {
		assert.NoError(t, run(starterConfigs, conf, bootstrapper, prometheus.NewRegistry(), prometheus.NewRegistry()))
	}()

	cc, err := client.Dial("unix://"+conf.SocketPath, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, cc.Close()) }()
	repoClient := gitalypb.NewRepositoryServiceClient(cc)

	ctx, cancel := testhelper.Context()
	defer cancel()

	createRepo := func(t *testing.T, storageName, relativePath string) *gitalypb.Repository {
		t.Helper()
		repo := &gitalypb.Repository{
			StorageName:  storageName,
			RelativePath: relativePath,
		}
		for i := 0; true; i++ {
			_, err := repoClient.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{Repository: repo})
			if err != nil {
				require.Regexp(t, "(no healthy nodes)|(no such file or directory)|(connection refused)", err.Error())
				require.Less(t, i, 100, "praefect doesn't serve for too long")
				time.Sleep(50 * time.Millisecond)
			} else {
				break
			}
		}

		if os.Getenv("GITALY_TEST_PRAEFECT_BIN") != "" {
			// This is an ad hoc workaround for the c61cdadb5 (Replace in-memory queue
			// with Postgres implementation, 2021-07-14). Because the workaround creates
			// record for storage using virtual_storage name, but we need to different, actual
			// storage names here.
			rs := datastore.NewPostgresRepositoryStore(db, nil)
			err := rs.DeleteRepository(ctx, storageName, relativePath, []string{storageName})
			require.NoError(t, err)
			err = rs.CreateRepository(ctx, storageName, relativePath, "gitaly-1", []string{"gitaly-2"}, nil, true, true)
			require.NoError(t, err)
		}

		return repo
	}

	praefectStorage := conf.VirtualStorages[0].Name

	t.Run("ok", func(t *testing.T) {
		repo := createRepo(t, praefectStorage, "path/to/test/repo")
		cmd := &removeRepository{
			logger:         testhelper.NewTestLogger(t),
			virtualStorage: repo.StorageName,
			relativePath:   repo.RelativePath,
			timeout:        defaultDialTimeout,
		}
		require.NoError(t, cmd.Exec(flag.NewFlagSet("", flag.PanicOnError), conf))

		require.NoDirExists(t, filepath.Join(g1Cfg.Storages[0].Path, repo.RelativePath))
		require.NoDirExists(t, filepath.Join(g2Cfg.Storages[0].Path, repo.RelativePath))

		var repositoryRowExists bool
		require.NoError(t, db.QueryRow(
			`SELECT EXISTS(SELECT FROM repositories WHERE virtual_storage = $1 AND relative_path = $2)`,
			cmd.virtualStorage, cmd.relativePath,
		).Scan(&repositoryRowExists))
		require.False(t, repositoryRowExists)
	})

	t.Run("no info about repository on praefect", func(t *testing.T) {
		repo := createRepo(t, praefectStorage, "path/to/test/repo")
		repoStore := datastore.NewPostgresRepositoryStore(db.DB, nil)
		require.NoError(t, repoStore.DeleteRepository(
			ctx, repo.StorageName, repo.RelativePath, []string{g1Cfg.Storages[0].Name},
		))
		require.NoError(t, repoStore.DeleteRepository(
			ctx, repo.StorageName, repo.RelativePath, []string{g2Cfg.Storages[0].Name},
		))

		logger := testhelper.NewTestLogger(t)
		loggerHook := test.NewLocal(logger)
		cmd := &removeRepository{
			logger:         logrus.NewEntry(logger),
			virtualStorage: praefectStorage,
			relativePath:   repo.RelativePath,
			timeout:        defaultDialTimeout,
		}
		require.NoError(t, cmd.Exec(flag.NewFlagSet("", flag.PanicOnError), conf))
		var found bool
		for _, entry := range loggerHook.AllEntries() {
			if strings.Contains(entry.Message, "praefect database has no info about the repository") {
				found = true
			}
		}
		require.True(t, found, "no expected message in the log")

		require.NoDirExists(t, filepath.Join(g1Cfg.Storages[0].Path, repo.RelativePath))
		require.NoDirExists(t, filepath.Join(g2Cfg.Storages[0].Path, repo.RelativePath))

		requireNoDatabaseInfo(t, db, cmd)
	})

	t.Run("one of gitalies is out of service", func(t *testing.T) {
		repo := createRepo(t, praefectStorage, "path/to/test/repo")
		g2Srv.Shutdown()

		logger := testhelper.NewTestLogger(t)
		loggerHook := test.NewLocal(logger)
		cmd := &removeRepository{
			logger:         logrus.NewEntry(logger),
			virtualStorage: praefectStorage,
			relativePath:   repo.RelativePath,
			timeout:        time.Second,
		}

		for {
			err := cmd.Exec(flag.NewFlagSet("", flag.PanicOnError), conf)
			if err == nil {
				break
			}
			regexp := "(transport: Error while dialing dial unix /" + strings.TrimPrefix(g2Srv.Address(), "unix:/") + ")|(primary gitaly is not healthy)"
			require.Regexp(t, regexp, err.Error())
			time.Sleep(time.Second)
		}

		var found bool
		for _, entry := range loggerHook.AllEntries() {
			if strings.Contains(entry.Message, `repository removal failed for gitaly "gitaly-2"`) {
				found = true
				break
			}
		}
		require.True(t, found, "no expected message in the log")

		require.NoDirExists(t, filepath.Join(g1Cfg.Storages[0].Path, repo.RelativePath))
		require.DirExists(t, filepath.Join(g2Cfg.Storages[0].Path, repo.RelativePath))

		requireNoDatabaseInfo(t, db, cmd)
	})

	bootstrapper.Terminate()
}

func requireNoDatabaseInfo(t *testing.T, db glsql.DB, cmd *removeRepository) {
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
	const (
		virtualStorage = "praefect"
		relativePath   = "relative_path/to/repo.git"
	)

	ctx, cancel := testhelper.Context()
	defer cancel()

	db := glsql.GetDB(t)

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

	ticker := helper.NewTimerTicker(time.Millisecond)
	defer ticker.Stop()

	errChan := make(chan error, 1)
	go func() {
		cmd := &removeRepository{virtualStorage: virtualStorage, relativePath: relativePath}
		errChan <- cmd.removeReplicationEvents(ctx, testhelper.NewTestLogger(t), db.DB, ticker)
	}()
	go func() {
		// We acknowledge in_progress job, so it unblocks the waiting loop.
		acks, err := queue.Acknowledge(ctx, datastore.JobStateCompleted, []uint64{inProgressEvent.ID})
		if assert.NoError(t, err) {
			assert.Equal(t, []uint64{inProgress1[0].ID}, acks)
		}
	}()

	for checkChan, exists := errChan, true; exists; {
		select {
		case err := <-checkChan:
			require.NoError(t, err)
			close(errChan)
			checkChan = nil
		default:
		}
		// Wait until job removed
		row := db.QueryRow(`SELECT EXISTS(SELECT FROM replication_queue WHERE id = $1)`, failedEvent.ID)
		require.NoError(t, row.Scan(&exists))
	}
	// Once there are no in_progress jobs anymore the method returns.
	require.NoError(t, <-errChan)

	var notExists bool
	row := db.QueryRow(`SELECT NOT EXISTS(SELECT FROM replication_queue)`)
	require.NoError(t, row.Scan(&notExists))
	require.True(t, notExists)
}
