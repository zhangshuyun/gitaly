package praefect

import (
	"context"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	gitaly_config "gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	serverPkg "gitlab.com/gitlab-org/gitaly/internal/server"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

// TestReplicatorProcessJobs verifies that a replicator will schedule jobs for
// all whitelisted repos
func TestReplicatorProcessJobsWhitelist(t *testing.T) {
	datastore := NewMemoryDatastore(config.Config{
		Nodes: []*models.Node{
			&models.Node{
				ID:      1,
				Address: "tcp://gitaly-primary.example.com",
				Storage: "praefect-internal-1",
			}, &models.Node{
				ID:      2,
				Address: "tcp://gitaly-backup1.example.com",
				Storage: "praefect-internal-2",
			}},
		Whitelist: []string{"abcd1234", "edfg5678"},
	})

	coordinator := NewCoordinator(logrus.New(), datastore)
	resultsCh := make(chan result)
	replman := NewReplMgr(
		"default",
		logrus.New(),
		datastore,
		coordinator,
		WithReplicator(&mockReplicator{resultsCh}),
	)

	for _, node := range datastore.storageNodes.m {
		err := coordinator.RegisterNode(node.Storage, node.Address)
		require.NoError(t, err)
	}

	ctx, cancel := testhelper.Context()

	errQ := make(chan error)

	go func() {
		errQ <- replman.ProcessBacklog(ctx)
	}()

	success := make(chan struct{})

	var expectedResults []result
	// we expect one job per whitelisted repo with each backend server
	for _, shard := range datastore.repositories.m {
		for _, secondary := range shard.Replicas {
			cc, err := coordinator.GetConnection(secondary.Storage)
			require.NoError(t, err)
			expectedResults = append(expectedResults,
				result{relativePath: shard.RelativePath,
					targetStorage: secondary.Storage,
					targetCC:      cc,
				})
		}
	}

	go func() {
		// we expect one job per whitelisted repo with each backend server
		for _, shard := range datastore.repositories.m {
			for range shard.Replicas {
				result := <-resultsCh
				assert.Contains(t, expectedResults, result)
			}
		}

		cancel()
		require.EqualError(t, <-errQ, context.Canceled.Error())
		success <- struct{}{}
	}()

	select {

	case <-success:
		return

	case <-time.After(time.Second):
		t.Fatalf("unable to iterate over expected jobs")

	}

}

type result struct {
	relativePath  string
	targetStorage string
	targetCC      *grpc.ClientConn
}

type mockReplicator struct {
	resultsCh chan<- result
}

func (mr *mockReplicator) Replicate(ctx context.Context, job ReplJob, target *grpc.ClientConn) error {
	select {

	case mr.resultsCh <- result{job.Repository.RelativePath, job.TargetNode.Storage, target}:
		return nil

	case <-ctx.Done():
		return ctx.Err()

	}

	return nil
}

func TestReplicate(t *testing.T) {
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
		gitaly_config.ModifyStorages(oldStorages)
	}()

	gitaly_config.ModifyStorages(append(gitaly_config.Config.Storages, gitaly_config.Storage{
		Name: backupStorageName,
		Path: backupDir,
	},
		gitaly_config.Storage{
			Name: "default",
			Path: testhelper.GitlabTestStoragePath(),
		},
	))

	ctx, cancel := testhelper.Context()
	defer cancel()

	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials(testhelper.RepositoryAuthToken)),
	}
	conn, err := grpc.Dial(srvSocketPath, connOpts...)
	require.NoError(t, err)

	commitID := testhelper.CreateCommit(t, testRepoPath, "master", &testhelper.CreateCommitOpts{
		Message: "a commit",
	})

	ctx, err = helper.InjectGitalyServers(ctx, "default", srvSocketPath, testhelper.RepositoryAuthToken)
	require.NoError(t, err)

	var replicator defaultReplicator
	require.NoError(t, replicator.Replicate(
		ctx,
		ReplJob{
			Repository: models.Repository{
				RelativePath: testRepo.GetRelativePath(),
			},
			SourceNode: models.Node{
				Storage: "default",
			},
			TargetNode: models.Node{
				Storage: backupStorageName,
			},
		},
		conn,
	))

	replicatedPath := filepath.Join(backupDir, filepath.Base(testRepoPath))
	testhelper.MustRunCommand(t, nil, "git", "-C", replicatedPath, "cat-file", "-e", commitID)
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

var RubyServer *rubyserver.Server

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

	RubyServer, err = rubyserver.Start()
	if err != nil {
		log.Fatal(err)
	}
	defer RubyServer.Stop()

	return m.Run()
}
