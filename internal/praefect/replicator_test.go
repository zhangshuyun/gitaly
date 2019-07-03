package praefect_test

import (
	"context"
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
	"google.golang.org/grpc/metadata"

	gitaly_config "gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	serverPkg "gitlab.com/gitlab-org/gitaly/internal/server"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

// TestReplicatorProcessJobs verifies that a replicator will schedule jobs for
// all whitelisted repos
func TestReplicatorProcessJobsWhitelist(t *testing.T) {
	var (
		cfg = config.Config{
			Servers: []*config.GitalyServer{
				{
					Name:       "default",
					ListenAddr: "tcp://gitaly-primary.example.com",
				},
				{
					Name:       "backup1",
					ListenAddr: "tcp://gitaly-backup1.example.com",
				},
				{
					Name:       "backup2",
					ListenAddr: "tcp://gitaly-backup2.example.com",
				},
			},
			Whitelist: []string{
				"abcd1234",
				"edfg5678",
			},
		}
		datastore   = praefect.NewMemoryDatastore(cfg)
		coordinator = praefect.NewCoordinator(logrus.New(), datastore)
		resultsCh   = make(chan result)
		replman     = praefect.NewReplMgr(
			cfg.Servers[2].Name,
			logrus.New(),
			datastore,
			coordinator,
			praefect.WithWhitelist(cfg.Whitelist),
			praefect.WithReplicator(&mockReplicator{resultsCh}),
		)
	)

	for _, node := range cfg.Servers {
		err := coordinator.RegisterNode(node.Name, node.ListenAddr)
		require.NoError(t, err)
	}

	ctx, cancel := testhelper.Context()

	errQ := make(chan error)

	go func() {
		errQ <- replman.ProcessBacklog(ctx)
	}()

	success := make(chan struct{})

	go func() {
		// we expect one job per whitelisted repo with each backend server
		for i := 0; i < len(cfg.Whitelist); i++ {
			result := <-resultsCh

			assert.Contains(t, cfg.Whitelist, result.source.RelativePath)
			assert.Equal(t, cfg.Servers[2].Name, result.target.Storage)
			assert.Equal(t, cfg.Servers[0].Name, result.source.Storage)
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
	source praefect.Repository
	target praefect.Node
}

type mockReplicator struct {
	resultsCh chan<- result
}

func (mr *mockReplicator) Replicate(ctx context.Context, source praefect.Repository, target praefect.Node) error {
	select {

	case mr.resultsCh <- result{source, target}:
		return nil

	case <-ctx.Done():
		return ctx.Err()

	}

	return nil
}

func TestReplicate(t *testing.T) {
	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	commitID := testhelper.CreateCommit(t, testRepoPath, "master", &testhelper.CreateCommitOpts{
		Message: "a commit",
	})

	defer cleanupFn()
	var (
		cfg = config.Config{
			Servers: []*config.GitalyServer{
				{
					Name:       "default",
					ListenAddr: "tcp://gitaly-primary.example.com",
				},
				{
					Name:       "backup",
					ListenAddr: "tcp://gitaly-backup1.example.com",
				},
			},
			Whitelist: []string{
				testRepo.GetRelativePath(),
			},
		}
	)
	backupDir := filepath.Join(testhelper.GitlabTestStoragePath(), "backup")
	require.NoError(t, os.Mkdir(backupDir, os.ModeDir|0755))
	defer func() {
		os.RemoveAll(backupDir)
	}()

	oldStorages := gitaly_config.Config.Storages
	defer func() {
		gitaly_config.Config.Storages = oldStorages
	}()

	gitaly_config.Config.Storages = append(gitaly_config.Config.Storages, gitaly_config.Storage{
		Name: "backup",
		Path: backupDir,
	}, gitaly_config.Storage{
		Name: "default",
		Path: testhelper.GitlabTestStoragePath(),
	})

	srv, socketPath := runFullGitalyServer(t)
	defer srv.Stop()

	datastore := praefect.NewMemoryDatastore(cfg)
	coordinator := praefect.NewCoordinator(logrus.New(), datastore)

	coordinator.RegisterNode("backup", socketPath)
	coordinator.RegisterNode("default", socketPath)

	replman := praefect.NewReplMgr(
		cfg.Servers[1].Name,
		logrus.New(),
		datastore,
		coordinator,
		praefect.WithWhitelist([]string{testRepo.GetRelativePath()}),
	)

	ctx, cancel := testhelper.Context()
	defer cancel()

	md := testhelper.GitalyServersMetadata(t, socketPath)
	ctx = metadata.NewOutgoingContext(ctx, md)

	go func() {
		require.Error(t, context.Canceled, replman.ProcessBacklog(ctx))
	}()
	var tries int
	jobs, err := datastore.GetJobs(praefect.JobStateInProgress|praefect.JobStatePending|praefect.JobStateReady, "backup", 1)
	require.NoError(t, err)

	for len(jobs) > 0 {
		if tries > 20 {
			t.Error("exceeded timeout")
		}
		time.Sleep(1 * time.Second)
		tries++

		jobs, err = datastore.GetJobs(praefect.JobStateInProgress|praefect.JobStatePending|praefect.JobStateReady, "backup", 1)
		require.NoError(t, err)
	}
	cancel()

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
