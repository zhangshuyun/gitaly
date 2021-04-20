package repository

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	gclient "gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/backchannel"
	dcache "gitlab.com/gitlab-org/gitaly/internal/cache"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	internalclient "gitlab.com/gitlab-org/gitaly/internal/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/commit"
	hookservice "gitlab.com/gitlab-org/gitaly/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/remote"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/ssh"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
	mcache "gitlab.com/gitlab-org/gitaly/internal/middleware/cache"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
)

// Stamp taken from https://golang.org/pkg/time/#pkg-constants
const testTimeString = "200601021504.05"

var (
	testTime   = time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC)
	RubyServer *rubyserver.Server
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	cleanup := testhelper.Configure()
	defer cleanup()

	config.Config.Auth.Token = testhelper.RepositoryAuthToken

	var err error
	config.Config.GitlabShell.Dir, err = filepath.Abs("testdata/gitlab-shell")
	if err != nil {
		log.Error(err)
		return 1
	}

	testhelper.ConfigureGitalyHooksBinary(config.Config.BinDir)
	testhelper.ConfigureGitalySSH(config.Config.BinDir)

	RubyServer = rubyserver.New(config.Config)
	if err := RubyServer.Start(); err != nil {
		log.Error(err)
		return 1
	}
	defer RubyServer.Stop()

	return m.Run()
}

func TestWithRubySidecar(t *testing.T) {
	cfg := testcfg.Build(t)

	testhelper.ConfigureGitalyHooksBin(t, cfg)

	rubySrv := rubyserver.New(cfg)
	require.NoError(t, rubySrv.Start())
	t.Cleanup(rubySrv.Stop)

	fs := []func(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server){
		testCloneFromPoolHTTP,
		testSetConfig,
		testFetchRemoteFailure,
		testFetchRemoteOverHTTP,
	}
	for _, f := range fs {
		t.Run(runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name(), func(t *testing.T) {
			f(t, cfg, rubySrv)
		})
	}
}

func newRepositoryClient(t testing.TB, cfg config.Cfg, serverSocketPath string) (gitalypb.RepositoryServiceClient, *grpc.ClientConn) {
	var connOpts []grpc.DialOption
	if cfg.Auth.Token != "" {
		connOpts = append(connOpts, grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(cfg.Auth.Token)))
	}
	conn, err := gclient.Dial(serverSocketPath, connOpts)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewRepositoryServiceClient(conn), conn
}

func newMuxedRepositoryClient(t *testing.T, ctx context.Context, cfg config.Cfg, serverSocketPath string, handshaker internalclient.Handshaker) gitalypb.RepositoryServiceClient {
	conn, err := internalclient.Dial(ctx, serverSocketPath, []grpc.DialOption{
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(cfg.Auth.Token)),
	}, handshaker)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return gitalypb.NewRepositoryServiceClient(conn)
}

var NewRepositoryClient = newRepositoryClient
var RunRepoServer = runRepoServer

func setupRepositoryServiceWithRuby(t testing.TB, cfg config.Cfg, rubySrv *rubyserver.Server) (config.Cfg, *gitalypb.Repository, string, gitalypb.RepositoryServiceClient) {
	client, serverSocketPath := runRepositoryService(t, cfg, rubySrv)
	cfg.SocketPath = serverSocketPath

	repo, repoPath, cleanup := gittest.CloneRepoAtStorage(t, cfg.Storages[0], t.Name())
	t.Cleanup(cleanup)

	return cfg, repo, repoPath, client
}

func runRepoServerWithConfig(t *testing.T, cfg config.Cfg, locator storage.Locator, opts ...testhelper.TestServerOpt) (string, func()) {
	streamInt := []grpc.StreamServerInterceptor{
		mcache.StreamInvalidator(dcache.NewLeaseKeyer(locator), protoregistry.GitalyProtoPreregistered),
	}
	unaryInt := []grpc.UnaryServerInterceptor{
		mcache.UnaryInvalidator(dcache.NewLeaseKeyer(locator), protoregistry.GitalyProtoPreregistered),
	}

	registry := backchannel.NewRegistry()
	txManager := transaction.NewManager(cfg, registry)
	hookManager := hook.NewManager(locator, txManager, hook.GitlabAPIStub, cfg)
	gitCmdFactory := git.NewExecCommandFactory(cfg)

	srv := testhelper.NewServerWithAuth(t, streamInt, unaryInt, cfg.Auth.Token, registry, opts...)
	gitalypb.RegisterRepositoryServiceServer(srv.GrpcServer(), NewServer(cfg, RubyServer, locator, txManager, gitCmdFactory))
	gitalypb.RegisterHookServiceServer(srv.GrpcServer(), hookservice.NewServer(cfg, hookManager, gitCmdFactory))
	srv.Start(t)

	return "unix://" + srv.Socket(), srv.Stop
}

func runRepoServer(t *testing.T, cfg config.Cfg, locator storage.Locator, opts ...testhelper.TestServerOpt) (string, func()) {
	return runRepoServerWithConfig(t, cfg, locator, opts...)
}

func assertModTimeAfter(t *testing.T, afterTime time.Time, paths ...string) bool {
	// NOTE: Since some filesystems don't have sub-second precision on `mtime`
	//       we're rounding the times to seconds
	afterTime = afterTime.Round(time.Second)
	for _, path := range paths {
		s, err := os.Stat(path)
		assert.NoError(t, err)

		if !s.ModTime().Round(time.Second).After(afterTime) {
			t.Errorf("ModTime is not after afterTime: %q < %q", s.ModTime().Round(time.Second).String(), afterTime.String())
		}
	}
	return t.Failed()
}

func runRepositoryServerWithConfig(t testing.TB, cfg config.Cfg, rubySrv *rubyserver.Server, opts ...testserver.GitalyServerOpt) string {
	return testserver.RunGitalyServer(t, cfg, rubySrv, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterRepositoryServiceServer(srv, NewServer(cfg, deps.GetRubyServer(), deps.GetLocator(), deps.GetTxManager(), deps.GetGitCmdFactory()))
		gitalypb.RegisterHookServiceServer(srv, hookservice.NewServer(cfg, deps.GetHookManager(), deps.GetGitCmdFactory()))
		gitalypb.RegisterRemoteServiceServer(srv, remote.NewServer(cfg, rubySrv, deps.GetLocator(), deps.GetGitCmdFactory()))
		gitalypb.RegisterSSHServiceServer(srv, ssh.NewServer(cfg, deps.GetLocator(), deps.GetGitCmdFactory()))
		gitalypb.RegisterRefServiceServer(srv, ref.NewServer(cfg, deps.GetLocator(), deps.GetGitCmdFactory(), deps.GetTxManager()))
		gitalypb.RegisterCommitServiceServer(srv, commit.NewServer(cfg, deps.GetLocator(), deps.GetGitCmdFactory(), nil))
	}, opts...)
}

func runRepositoryService(t testing.TB, cfg config.Cfg, rubySrv *rubyserver.Server, opts ...testserver.GitalyServerOpt) (gitalypb.RepositoryServiceClient, string) {
	serverSocketPath := runRepositoryServerWithConfig(t, cfg, rubySrv, opts...)
	client, conn := newRepositoryClient(t, cfg, serverSocketPath)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	return client, serverSocketPath
}

func setupRepositoryService(t testing.TB, opts ...testserver.GitalyServerOpt) (config.Cfg, *gitalypb.Repository, string, gitalypb.RepositoryServiceClient) {
	cfg, client := setupRepositoryServiceWithoutRepo(t, opts...)
	repo, repoPath, cleanup := gittest.CloneRepoAtStorage(t, cfg.Storages[0], t.Name())
	t.Cleanup(cleanup)
	return cfg, repo, repoPath, client
}

func setupRepositoryServiceWithoutRepo(t testing.TB, opts ...testserver.GitalyServerOpt) (config.Cfg, gitalypb.RepositoryServiceClient) {
	cfg := testcfg.Build(t)

	testhelper.ConfigureGitalyHooksBin(t, cfg)
	testhelper.ConfigureGitalySSHBin(t, cfg)

	client, serverSocketPath := runRepositoryService(t, cfg, nil, opts...)
	cfg.SocketPath = serverSocketPath

	return cfg, client
}

func setupRepositoryServiceWithWorktree(t testing.TB) (config.Cfg, *gitalypb.Repository, string, gitalypb.RepositoryServiceClient) {
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	repo, repoPath, cleanup := gittest.CloneRepoWithWorktreeAtStorage(t, cfg.Storages[0])
	t.Cleanup(cleanup)

	return cfg, repo, repoPath, client
}
