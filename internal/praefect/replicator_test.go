package praefect

import (
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	gitaly_config "gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/git/objectpool"
	repo "gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	serverPkg "gitlab.com/gitlab-org/gitaly/internal/server"
	objectpoolservice "gitlab.com/gitlab-org/gitaly/internal/service/objectpool"
	"gitlab.com/gitlab-org/gitaly/internal/service/remote"
	"gitlab.com/gitlab-org/gitaly/internal/service/repository"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/promtest"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
)

func TestProcessReplicationJob(t *testing.T) {
	srv, srvSocketPath := runFullGitalyServer(t)
	defer srv.Stop()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	backupStorageName := "backup"

	backupDir, err := ioutil.TempDir(testhelper.GitlabTestStoragePath(), backupStorageName)
	require.NoError(t, err)

	defer func() {
		os.RemoveAll(backupDir)
	}()

	oldStorages := gitaly_config.Config.Storages
	defer func() {
		gitaly_config.Config.Storages = oldStorages
	}()

	gitaly_config.Config.Storages = append(gitaly_config.Config.Storages, gitaly_config.Storage{
		Name: backupStorageName,
		Path: backupDir,
	},
		gitaly_config.Storage{
			Name: "default",
			Path: testhelper.GitlabTestStoragePath(),
		},
	)

	config := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			&config.VirtualStorage{
				Name: "default",
				Nodes: []*models.Node{
					&models.Node{
						Storage:        "default",
						Address:        srvSocketPath,
						Token:          gitaly_config.Config.Auth.Token,
						DefaultPrimary: true,
					},
					&models.Node{
						Storage: backupStorageName,
						Address: srvSocketPath,
						Token:   gitaly_config.Config.Auth.Token,
					},
				},
			},
		},
	}

	ds := datastore.NewInMemory(config)

	// create object pool on the source
	objectPoolPath := testhelper.NewTestObjectPoolName(t)
	pool, err := objectpool.NewObjectPool(testRepo.GetStorageName(), objectPoolPath)
	require.NoError(t, err)

	poolCtx, cancel := testhelper.Context()
	defer cancel()

	require.NoError(t, pool.Create(poolCtx, testRepo))
	require.NoError(t, pool.Link(poolCtx, testRepo))

	// replicate object pool repository to target node
	targetObjectPoolRepo := *pool.ToProto().GetRepository()
	targetObjectPoolRepo.StorageName = "backup"

	ctx, cancel := testhelper.Context()
	defer cancel()

	injectedCtx := metadata.NewOutgoingContext(ctx, testhelper.GitalyServersMetadata(t, srvSocketPath))

	repoClient, con := newRepositoryClient(t, srvSocketPath)
	defer con.Close()

	_, err = repoClient.ReplicateRepository(injectedCtx, &gitalypb.ReplicateRepositoryRequest{
		Repository: &targetObjectPoolRepo,
		Source:     pool.ToProto().GetRepository(),
	})
	require.NoError(t, err)

	primary, err := ds.GetPrimary(config.VirtualStorages[0].Name)
	require.NoError(t, err)
	secondaries, err := ds.GetSecondaries(config.VirtualStorages[0].Name)
	require.NoError(t, err)

	var secondaryStorages []string
	for _, secondary := range secondaries {
		secondaryStorages = append(secondaryStorages, secondary.Storage)
	}
	_, err = ds.CreateReplicaReplJobs(testRepo.GetRelativePath(), primary.Storage, secondaryStorages, datastore.UpdateRepo)
	require.NoError(t, err)

	jobs, err := ds.GetJobs(datastore.JobStateReady|datastore.JobStatePending, backupStorageName, 1)
	require.NoError(t, err)
	require.Len(t, jobs, 1)

	commitID := testhelper.CreateCommit(t, testRepoPath, "master", &testhelper.CreateCommitOpts{
		Message: "a commit",
	})

	var replicator defaultReplicator
	entry := testhelper.DiscardTestEntry(t)
	replicator.log = entry

	nodeMgr, err := nodes.NewManager(entry, config)
	require.NoError(t, err)
	nodeMgr.Start(1*time.Millisecond, 5*time.Millisecond)

	var mockReplicationGauge promtest.MockGauge
	var mockReplicationHistogram promtest.MockHistogram

	replMgr := NewReplMgr("", testhelper.DiscardTestEntry(t), ds, nodeMgr, WithLatencyMetric(&mockReplicationHistogram), WithQueueMetric(&mockReplicationGauge))
	replMgr.replicator = replicator

	shard, err := nodeMgr.GetShard(config.VirtualStorages[0].Name)
	require.NoError(t, err)
	primaryNode, err := shard.GetPrimary()
	require.NoError(t, err)
	secondaryNodes, err := shard.GetSecondaries()
	require.NoError(t, err)
	require.Len(t, secondaryNodes, 1)

	replMgr.processReplJob(ctx, jobs[0], primaryNode.GetConnection(), secondaryNodes[0].GetConnection())

	relativeRepoPath, err := filepath.Rel(testhelper.GitlabTestStoragePath(), testRepoPath)
	require.NoError(t, err)
	replicatedPath := filepath.Join(backupDir, relativeRepoPath)

	testhelper.MustRunCommand(t, nil, "git", "-C", replicatedPath, "cat-file", "-e", commitID)
	testhelper.MustRunCommand(t, nil, "git", "-C", replicatedPath, "gc")
	require.Less(t, testhelper.GetGitPackfileDirSize(t, replicatedPath), int64(100), "expect a small pack directory")

	require.Equal(t, 1, mockReplicationGauge.IncsCalled())
	require.Equal(t, 1, mockReplicationGauge.DecsCalled())
	require.Len(t, mockReplicationHistogram.Values, 1)
}

func TestConfirmReplication(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	srv, srvSocketPath := runFullGitalyServer(t)
	defer srv.Stop()

	testRepoA, testRepoAPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testRepoB, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials(testhelper.RepositoryAuthToken)),
	}
	conn, err := grpc.Dial(srvSocketPath, connOpts...)
	require.NoError(t, err)

	var replicator defaultReplicator
	entry := testhelper.DiscardTestEntry(t)
	replicator.log = entry

	equal, err := replicator.confirmChecksums(ctx, gitalypb.NewRepositoryServiceClient(conn), gitalypb.NewRepositoryServiceClient(conn), testRepoA, testRepoB)
	require.NoError(t, err)
	require.True(t, equal)

	testhelper.CreateCommit(t, testRepoAPath, "master", &testhelper.CreateCommitOpts{
		Message: "a commit",
	})

	equal, err = replicator.confirmChecksums(ctx, gitalypb.NewRepositoryServiceClient(conn), gitalypb.NewRepositoryServiceClient(conn), testRepoA, testRepoB)
	require.NoError(t, err)
	require.False(t, equal)
}

func TestProcessBacklog_FailedJobs(t *testing.T) {
	primarySvr, primarySocket := newReplicationService(t)
	defer primarySvr.Stop()

	backupSvr, backupSocket := newReplicationService(t)
	backupSvr.Stop()

	internalListener, err := net.Listen("unix", gitaly_config.GitalyInternalSocketPath())
	require.NoError(t, err)
	go backupSvr.Serve(internalListener)

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	backupStorageName := "backup"

	backupDir, err := ioutil.TempDir(testhelper.GitlabTestStoragePath(), backupStorageName)
	require.NoError(t, err)

	defer os.RemoveAll(backupDir)

	primary := models.Node{
		Storage:        "default",
		Address:        "unix://" + primarySocket,
		DefaultPrimary: true,
	}

	secondary := models.Node{
		Storage: backupStorageName,
		Address: "unix://" + backupSocket,
	}

	config := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "default",
				Nodes: []*models.Node{
					&primary,
					&secondary,
				},
			},
		},
	}

	ctx, cancel := testhelper.Context()
	defer func(oldStorages []gitaly_config.Storage) {
		gitaly_config.Config.Storages = oldStorages
		cancel()
	}(gitaly_config.Config.Storages)

	gitaly_config.Config.Storages = append(gitaly_config.Config.Storages, gitaly_config.Storage{
		Name: backupStorageName,
		Path: backupDir,
	},
		gitaly_config.Storage{
			Name: "default",
			Path: testhelper.GitlabTestStoragePath(),
		},
	)

	ds := datastore.NewInMemory(config)
	ids, err := ds.CreateReplicaReplJobs(testRepo.GetRelativePath(), primary.Storage, []string{secondary.Storage}, datastore.UpdateRepo)
	require.NoError(t, err)
	require.Len(t, ids, 1)

	entry := testhelper.DiscardTestEntry(t)

	require.NoError(t, ds.UpdateReplJobState(ids[0], datastore.JobStateReady))

	nodeMgr, err := nodes.NewManager(entry, config)
	require.NoError(t, err)

	replMgr := NewReplMgr("default", entry, ds, nodeMgr)
	replMgr.replJobTimeout = 100 * time.Millisecond

	go replMgr.ProcessBacklog(ctx, noopBackoffFunc)

	timeLimit := time.NewTimer(5 * time.Second)
	ticker := time.NewTicker(1 * time.Second)

	// the job will fail to process because the client connection for "backup" is not registered. It should fail maxAttempts times
	// and get cancelled.
TestJobGetsCancelled:
	for {
		select {
		case <-ticker.C:
			replJobs, err := ds.GetJobs(datastore.JobStateDead, "backup", 10)
			require.NoError(t, err)
			if len(replJobs) == 1 {
				//success
				timeLimit.Stop()
				break TestJobGetsCancelled
			}
		case <-timeLimit.C:
			t.Fatal("time limit expired for job to be deemed dead")
		}
	}
}

func TestProcessBacklog_Success(t *testing.T) {
	primarySvr, primarySocket := newReplicationService(t)
	defer primarySvr.Stop()

	backupSvr, backupSocket := newReplicationService(t)
	defer backupSvr.Stop()

	internalListener, err := net.Listen("unix", gitaly_config.GitalyInternalSocketPath())
	require.NoError(t, err)
	go backupSvr.Serve(internalListener)

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	backupStorageName := "backup"

	backupDir, err := ioutil.TempDir(testhelper.GitlabTestStoragePath(), backupStorageName)
	require.NoError(t, err)

	defer os.RemoveAll(backupDir)

	primary := models.Node{
		Storage:        "default",
		Address:        "unix://" + primarySocket,
		DefaultPrimary: true,
	}

	secondary := models.Node{
		Storage: backupStorageName,
		Address: "unix://" + backupSocket,
	}

	config := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "default",
				Nodes: []*models.Node{
					&primary,
					&secondary,
				},
			},
		},
	}

	ctx, cancel := testhelper.Context()
	defer func(oldStorages []gitaly_config.Storage) {
		gitaly_config.Config.Storages = oldStorages
		cancel()
	}(gitaly_config.Config.Storages)

	gitaly_config.Config.Storages = append(gitaly_config.Config.Storages, gitaly_config.Storage{
		Name: backupStorageName,
		Path: backupDir,
	},
		gitaly_config.Storage{
			Name: "default",
			Path: testhelper.GitlabTestStoragePath(),
		},
	)

	ds := datastore.NewInMemory(config)
	ids, err := ds.CreateReplicaReplJobs(testRepo.GetRelativePath(), primary.Storage, []string{secondary.Storage}, datastore.UpdateRepo)
	require.NoError(t, err)
	require.Len(t, ids, 1)

	entry := testhelper.DiscardTestEntry(t)

	require.NoError(t, ds.UpdateReplJobState(ids[0], datastore.JobStateReady))

	nodeMgr, err := nodes.NewManager(entry, config)
	require.NoError(t, err)

	replMgr := NewReplMgr("default", entry, ds, nodeMgr)
	replMgr.replJobTimeout = 5 * time.Second

	go replMgr.ProcessBacklog(ctx, noopBackoffFunc)

	timeLimit := time.NewTimer(5 * time.Second)
	ticker := time.NewTicker(1 * time.Second)

	// Once the listener is being served, and we try the job again it should succeed
TestJobSucceeds:
	for {
		select {
		case <-ticker.C:
			replJobs, err := ds.GetJobs(datastore.JobStateFailed|datastore.JobStateInProgress|datastore.JobStateReady|datastore.JobStateDead, "backup", 10)
			require.NoError(t, err)
			if len(replJobs) == 0 {
				//success
				break TestJobSucceeds
			}
		case <-timeLimit.C:
			t.Fatal("time limit expired for job to complete")
		}
	}
}

func TestBackoff(t *testing.T) {
	start := 1 * time.Microsecond
	max := 6 * time.Microsecond
	expectedBackoffs := []time.Duration{
		1 * time.Microsecond,
		2 * time.Microsecond,
		4 * time.Microsecond,
		6 * time.Microsecond,
		6 * time.Microsecond,
		6 * time.Microsecond,
	}
	b, reset := ExpBackoffFunc(start, max)()
	for _, expectedBackoff := range expectedBackoffs {
		require.Equal(t, expectedBackoff, b())
	}

	reset()
	require.Equal(t, start, b())
}

func runFullGitalyServer(t *testing.T) (*grpc.Server, string) {
	server := serverPkg.NewInsecure(RubyServer)

	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName()

	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}
	//listen on internal socket
	internalListener, err := net.Listen("unix", gitaly_config.GitalyInternalSocketPath())
	require.NoError(t, err)

	go server.Serve(listener)
	go server.Serve(internalListener)

	return server, "unix://" + serverSocketPath
}

// newReplicationService is a grpc service that has the Repository, Remote and ObjectPool services, which
// are the only ones needed for replication
func newReplicationService(tb testing.TB) (*grpc.Server, string) {
	socketName := testhelper.GetTemporaryGitalySocketFileName()

	svr := testhelper.NewTestGrpcServer(tb, nil, nil)

	gitalypb.RegisterRepositoryServiceServer(svr, repository.NewServer(repo.NewTransactionManager(), &rubyserver.Server{}))
	gitalypb.RegisterObjectPoolServiceServer(svr, objectpoolservice.NewServer())
	gitalypb.RegisterRemoteServiceServer(svr, remote.NewServer(&rubyserver.Server{}))
	reflection.Register(svr)

	listener, err := net.Listen("unix", socketName)
	require.NoError(tb, err)

	go svr.Serve(listener)

	return svr, socketName
}

func newRepositoryClient(t *testing.T, serverSocketPath string) (gitalypb.RepositoryServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials(testhelper.RepositoryAuthToken)),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewRepositoryServiceClient(conn), conn
}

var RubyServer = &rubyserver.Server{}

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	gitaly_config.Config.Auth.Token = testhelper.RepositoryAuthToken

	var err error
	gitaly_config.Config.GitlabShell.Dir, err = filepath.Abs("testdata/gitlab-shell")
	if err != nil {
		log.Fatal(err)
	}

	testhelper.ConfigureGitalySSH()

	if err := RubyServer.Start(); err != nil {
		log.Fatal(err)
	}
	defer RubyServer.Stop()

	return m.Run()
}
