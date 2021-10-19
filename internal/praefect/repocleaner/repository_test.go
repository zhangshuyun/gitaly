package repocleaner

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/service/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
)

func TestRunner_Run(t *testing.T) {
	t.Parallel()

	const (
		repo1RelPath = "repo-1.git"
		repo2RelPath = "repo-2.git"
		repo3RelPath = "repo-3.git"

		storage1 = "gitaly-1"
		storage2 = "gitaly-2"
		storage3 = "gitaly-3"

		virtualStorage = "praefect"
	)

	g1Cfg := testcfg.Build(t, testcfg.WithStorages(storage1))
	g2Cfg := testcfg.Build(t, testcfg.WithStorages(storage2))
	g3Cfg := testcfg.Build(t, testcfg.WithStorages(storage3))

	g1Addr := testserver.RunGitalyServer(t, g1Cfg, nil, setup.RegisterAll, testserver.WithDisablePraefect())
	g2Addr := testserver.RunGitalyServer(t, g2Cfg, nil, setup.RegisterAll, testserver.WithDisablePraefect())
	g3Addr := testserver.RunGitalyServer(t, g3Cfg, nil, setup.RegisterAll, testserver.WithDisablePraefect())

	db := glsql.NewDB(t)
	dbConf := glsql.GetDBConfig(t, db.Name)

	conf := config.Config{
		SocketPath: testhelper.GetTemporaryGitalySocketFileName(t),
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: virtualStorage,
				Nodes: []*config.Node{
					{Storage: g1Cfg.Storages[0].Name, Address: g1Addr},
					{Storage: g2Cfg.Storages[0].Name, Address: g2Addr},
					{Storage: g3Cfg.Storages[0].Name, Address: g3Addr},
				},
			},
		},
		DB: dbConf,
	}
	cfg := Cfg{
		RunInterval:         time.Duration(1),
		LivenessInterval:    time.Duration(1),
		RepositoriesInBatch: 2,
	}

	gittest.CloneRepo(t, g1Cfg, g1Cfg.Storages[0], gittest.CloneRepoOpts{RelativePath: repo1RelPath})
	gittest.CloneRepo(t, g1Cfg, g1Cfg.Storages[0], gittest.CloneRepoOpts{RelativePath: repo2RelPath})
	gittest.CloneRepo(t, g1Cfg, g1Cfg.Storages[0], gittest.CloneRepoOpts{RelativePath: repo3RelPath})

	// second gitaly is missing repo-3.git repository
	gittest.CloneRepo(t, g2Cfg, g2Cfg.Storages[0], gittest.CloneRepoOpts{RelativePath: repo1RelPath})
	gittest.CloneRepo(t, g2Cfg, g2Cfg.Storages[0], gittest.CloneRepoOpts{RelativePath: repo2RelPath})

	// third gitaly has an extra repo-4.git repository
	gittest.CloneRepo(t, g3Cfg, g3Cfg.Storages[0], gittest.CloneRepoOpts{RelativePath: repo1RelPath})
	gittest.CloneRepo(t, g3Cfg, g3Cfg.Storages[0], gittest.CloneRepoOpts{RelativePath: repo2RelPath})
	gittest.CloneRepo(t, g3Cfg, g3Cfg.Storages[0], gittest.CloneRepoOpts{RelativePath: repo3RelPath})
	gittest.CloneRepo(t, g3Cfg, g3Cfg.Storages[0], gittest.CloneRepoOpts{RelativePath: "repo-4.git"})

	ctx, cancel := testhelper.Context()
	defer cancel()

	repoStore := datastore.NewPostgresRepositoryStore(db.DB, nil)
	for _, set := range []struct {
		relativePath string
		primary      string
		secondaries  []string
	}{
		{
			relativePath: repo1RelPath,
			primary:      storage1,
			secondaries:  []string{storage3},
		},
		{
			relativePath: repo2RelPath,
			primary:      storage1,
			secondaries:  []string{storage2, storage3},
		},
		{
			relativePath: repo3RelPath,
			primary:      storage1,
			secondaries:  []string{storage2, storage3},
		},
	} {
		id, err := repoStore.ReserveRepositoryID(ctx, conf.VirtualStorages[0].Name, set.relativePath)
		require.NoError(t, err)
		require.NoError(t, repoStore.CreateRepository(ctx, id, conf.VirtualStorages[0].Name, set.relativePath, set.primary, set.secondaries, nil, false, false))
	}

	logger, loggerHook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)

	entry := logger.WithContext(ctx)
	clientHandshaker := backchannel.NewClientHandshaker(entry, praefect.NewBackchannelServerFactory(entry, transaction.NewServer(nil), nil))
	nodeSet, err := praefect.DialNodes(ctx, conf.VirtualStorages, protoregistry.GitalyProtoPreregistered, nil, clientHandshaker, nil)
	require.NoError(t, err)
	defer nodeSet.Close()

	storageCleanup := datastore.NewStorageCleanup(db.DB)

	var iteration int32
	runner := NewRunner(cfg, logger, praefect.StaticHealthChecker{virtualStorage: []string{storage1, storage2, storage3}}, nodeSet.Connections(), storageCleanup, storageCleanup, actionStub{
		PerformMethod: func(ctx context.Context, notExisting []datastore.RepositoryClusterPath) error {
			// Because action invocation happens for batches each run could result
			// multiple invocations of the action. Amount of invocations can be calculated
			// as amount of storage repositories divided on the size of the batch and rounded up.
			// For storages:
			//	'gitaly-1' is it 3 repos / 2 = 2 calls [0,1]
			// 	'gitaly-2' is it 2 repos / 2 = 1 call [2]
			// 	'gitaly-3' is it 4 repos / 2 = 2 calls [3,4]
			i := atomic.LoadInt32(&iteration)
			switch i {
			case 0, 1:
				assert.ElementsMatch(t, nil, notExisting)
			case 2:
				assert.ElementsMatch(
					t,
					[]datastore.RepositoryClusterPath{
						datastore.NewRepositoryClusterPath(virtualStorage, storage2, repo1RelPath),
					},
					notExisting,
				)
			case 3:
				assert.ElementsMatch(t, nil, notExisting)
			case 4:
				// Terminates the loop.
				defer cancel()
				assert.Equal(
					t,
					[]datastore.RepositoryClusterPath{
						datastore.NewRepositoryClusterPath(virtualStorage, storage3, "repo-4.git"),
					},
					notExisting,
				)
				return nil
			}
			atomic.AddInt32(&iteration, 1)
			return nil
		},
	})

	ticker := helper.NewManualTicker()
	done := make(chan struct{})
	go func() {
		defer close(done)
		assert.Equal(t, context.Canceled, runner.Run(ctx, ticker))
	}()
	// We have 3 storages that is why it requires 3 runs to cover them all.
	// As the first run happens automatically we need to make 2 ticks for 2 additional runs.
	for i := 0; i < len(conf.VirtualStorages[0].Nodes)-1; i++ {
		ticker.Tick()
	}
	waitReceive(t, done)

	require.Equal(t, int32(4), atomic.LoadInt32(&iteration))
	require.GreaterOrEqual(t, len(loggerHook.AllEntries()), 2)
	require.Equal(
		t,
		map[string]interface{}{"Data": logrus.Fields{"component": "repocleaner.repository_existence"}, "Message": "started"},
		map[string]interface{}{"Data": loggerHook.AllEntries()[0].Data, "Message": loggerHook.AllEntries()[0].Message},
	)
	require.Equal(
		t,
		map[string]interface{}{"Data": logrus.Fields{"component": "repocleaner.repository_existence"}, "Message": "completed"},
		map[string]interface{}{"Data": loggerHook.LastEntry().Data, "Message": loggerHook.LastEntry().Message},
	)
}

func TestRunner_Run_noAvailableStorages(t *testing.T) {
	t.Parallel()

	const (
		repo1RelPath   = "repo-1.git"
		storage1       = "gitaly-1"
		virtualStorage = "praefect"
	)

	g1Cfg := testcfg.Build(t, testcfg.WithStorages(storage1))
	g1Addr := testserver.RunGitalyServer(t, g1Cfg, nil, setup.RegisterAll, testserver.WithDisablePraefect())

	db := glsql.NewDB(t)
	dbConf := glsql.GetDBConfig(t, db.Name)

	conf := config.Config{
		SocketPath: testhelper.GetTemporaryGitalySocketFileName(t),
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: virtualStorage,
				Nodes: []*config.Node{
					{Storage: g1Cfg.Storages[0].Name, Address: g1Addr},
				},
			},
		},
		DB: dbConf,
	}
	cfg := Cfg{
		RunInterval:         time.Minute,
		LivenessInterval:    time.Second,
		RepositoriesInBatch: 2,
	}

	gittest.CloneRepo(t, g1Cfg, g1Cfg.Storages[0], gittest.CloneRepoOpts{RelativePath: repo1RelPath})

	ctx, cancel := testhelper.Context()
	defer cancel()

	repoStore := datastore.NewPostgresRepositoryStore(db.DB, nil)
	for i, set := range []struct {
		relativePath string
		primary      string
	}{
		{
			relativePath: repo1RelPath,
			primary:      storage1,
		},
	} {
		require.NoError(t, repoStore.CreateRepository(ctx, int64(i), conf.VirtualStorages[0].Name, set.relativePath, set.primary, nil, nil, false, false))
	}

	logger := testhelper.NewTestLogger(t)
	entry := logger.WithContext(ctx)
	clientHandshaker := backchannel.NewClientHandshaker(entry, praefect.NewBackchannelServerFactory(entry, transaction.NewServer(nil), nil))
	nodeSet, err := praefect.DialNodes(ctx, conf.VirtualStorages, protoregistry.GitalyProtoPreregistered, nil, clientHandshaker, nil)
	require.NoError(t, err)
	defer nodeSet.Close()

	storageCleanup := datastore.NewStorageCleanup(db.DB)
	startSecond := make(chan struct{})
	releaseFirst := make(chan struct{})
	runner := NewRunner(cfg, logger, praefect.StaticHealthChecker{virtualStorage: []string{storage1}}, nodeSet.Connections(), storageCleanup, storageCleanup, actionStub{
		PerformMethod: func(ctx context.Context, notExisting []datastore.RepositoryClusterPath) error {
			assert.Empty(t, notExisting)
			// Block execution here until second instance completes its execution.
			// It allows us to be sure the picked storage can't be picked once again by
			// another instance as well as that it works without problems if there is
			// nothing to pick up to process.
			close(startSecond)
			<-releaseFirst
			return nil
		},
	})

	go func() {
		// Continue execution of the first runner after the second completes.
		defer close(releaseFirst)

		logger, loggerHook := test.NewNullLogger()
		logger.SetLevel(logrus.DebugLevel)

		runner := NewRunner(cfg, logger, praefect.StaticHealthChecker{virtualStorage: []string{storage1}}, nodeSet.Connections(), storageCleanup, storageCleanup, actionStub{
			PerformMethod: func(ctx context.Context, notExisting []datastore.RepositoryClusterPath) error {
				assert.FailNow(t, "should not be triggered as there is no available storages to acquire")
				return nil
			},
		})

		ctx, cancel := testhelper.Context()
		defer cancel()
		ticker := helper.NewManualTicker()

		done := make(chan struct{})
		go func() {
			defer close(done)
			<-startSecond
			assert.Equal(t, context.Canceled, runner.Run(ctx, ticker))
		}()
		// It fills the buffer with the value and is a non-blocking call.
		ticker.Tick()
		// It blocks until buffered value is consumed, that mean the initial
		// run is completed and next one is started.
		ticker.Tick()
		cancel()
		<-done

		entries := loggerHook.AllEntries()
		require.Greater(t, len(entries), 2)
		require.Equal(
			t,
			map[string]interface{}{"Data": logrus.Fields{"component": "repocleaner.repository_existence"}, "Message": "no storages to verify"},
			map[string]interface{}{"Data": loggerHook.AllEntries()[1].Data, "Message": loggerHook.AllEntries()[1].Message},
		)
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		assert.Equal(t, context.Canceled, runner.Run(ctx, helper.NewManualTicker()))
	}()
	// Once the second runner completes we can proceed with execution of the first.
	waitReceive(t, releaseFirst)
	// Terminate the first runner.
	cancel()
	waitReceive(t, done)
}

type actionStub struct {
	PerformMethod func(ctx context.Context, existence []datastore.RepositoryClusterPath) error
}

func (as actionStub) Perform(ctx context.Context, existence []datastore.RepositoryClusterPath) error {
	if as.PerformMethod != nil {
		return as.PerformMethod(ctx, existence)
	}
	return nil
}

func waitReceive(t testing.TB, waitChan <-chan struct{}) {
	t.Helper()
	select {
	case <-waitChan:
	case <-time.After(time.Minute):
		require.FailNow(t, "waiting for too long")
	}
}
