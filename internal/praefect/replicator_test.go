package praefect

import (
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	gitaly_config "gitlab.com/gitlab-org/gitaly/internal/config"
	gitaly_log "gitlab.com/gitlab-org/gitaly/internal/log"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	serverPkg "gitlab.com/gitlab-org/gitaly/internal/server"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestProceessReplicationJob(t *testing.T) {
	mockReplicationLatency := &testhelper.MockHistogram{}
	mockReplicationGauge := &testhelper.MockGauge{}

	recordReplicationLatency = func(d float64) {
		mockReplicationLatency.Observe(d)
	}

	incReplicationJobsInFlight = func() {
		mockReplicationGauge.Inc()
	}

	decReplicationJobsInFlight = func() {
		mockReplicationGauge.Dec()
	}

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

	job := jobRecord{state: datastore.JobStateReady}

	m := &datastore.MemoryDatastore{
		jobs: &struct {
			sync.RWMutex
			records map[uint64]jobRecord // all jobs indexed by ID
		}{
			records: map[uint64]jobRecord{1: job},
		},
	}

	replJob := datastore.ReplJob{ID: 1,
		TargetNode: models.Node{Storage: backupStorageName, Address: srvSocketPath},
		SourceNode: models.Node{Storage: "default", Address: srvSocketPath, Token: testhelper.RepositoryAuthToken},
		Repository: models.Repository{Primary: models.Node{Storage: "default", Address: srvSocketPath}, RelativePath: testRepo.GetRelativePath()},
		State:      datastore.JobStateReady,
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	commitID := testhelper.CreateCommit(t, testRepoPath, "master", &testhelper.CreateCommitOpts{
		Message: "a commit",
	})

	var replicator defaultReplicator
	replicator.log = gitaly_log.Default()

	coordinator := &Coordinator{nodes: make(map[string]*grpc.ClientConn)}
	coordinator.RegisterNode("default", srvSocketPath)
	coordinator.RegisterNode("backup", srvSocketPath)

	replMgr := &ReplMgr{
		log:         gitaly_log.Default(),
		datastore:   m,
		coordinator: coordinator,
		replicator:  replicator,
	}

	require.NoError(t, replMgr.processReplJob(ctx, replJob))

	replicatedPath := filepath.Join(backupDir, filepath.Base(testRepoPath))
	testhelper.MustRunCommand(t, nil, "git", "-C", replicatedPath, "cat-file", "-e", commitID)

	require.Equal(t, 1, mockReplicationGauge.IncrementCalled)
	require.Equal(t, 1, mockReplicationGauge.DecrementCalled)
	require.Len(t, mockReplicationLatency.Values, 1)
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
	replicator.log = gitaly_log.Default()

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

func runFullGitalyServer(t *testing.T) (*grpc.Server, string) {
	server := serverPkg.NewInsecure(RubyServer)
	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName()

	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	go server.Serve(listener)

	return server, "unix://" + serverSocketPath
}

var RubyServer = &rubyserver.Server{}

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	gitaly_config.Config.Auth = gitaly_config.Auth{Token: testhelper.RepositoryAuthToken}

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
