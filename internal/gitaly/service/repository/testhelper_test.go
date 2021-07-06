package repository

import (
	"context"
	"os"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v14/auth"
	gclient "gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	internalclient "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/commit"
	hookservice "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/remote"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/ssh"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
)

// Stamp taken from https://golang.org/pkg/time/#pkg-constants
const testTimeString = "200601021504.05"

var testTime = time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	cleanup := testhelper.Configure()
	defer cleanup()

	return m.Run()
}

func TestWithRubySidecar(t *testing.T) {
	t.Parallel()
	cfg := testcfg.Build(t)

	testhelper.ConfigureGitalyHooksBin(t, cfg)

	rubySrv := rubyserver.New(cfg)
	require.NoError(t, rubySrv.Start())
	t.Cleanup(rubySrv.Stop)

	fs := []func(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server){
		testSetConfig,
		testSetConfigTransactional,
		testSuccessfulFindLicenseRequest,
		testFindLicenseRequestEmptyRepo,
	}
	for _, f := range fs {
		t.Run(runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name(), func(t *testing.T) {
			f(t, cfg, rubySrv)
		})
	}
}

func newRepositoryClient(t testing.TB, cfg config.Cfg, serverSocketPath string) gitalypb.RepositoryServiceClient {
	var connOpts []grpc.DialOption
	if cfg.Auth.Token != "" {
		connOpts = append(connOpts, grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(cfg.Auth.Token)))
	}
	conn, err := gclient.Dial(serverSocketPath, connOpts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	return gitalypb.NewRepositoryServiceClient(conn)
}

func newMuxedRepositoryClient(t *testing.T, ctx context.Context, cfg config.Cfg, serverSocketPath string, handshaker internalclient.Handshaker) gitalypb.RepositoryServiceClient {
	conn, err := internalclient.Dial(ctx, serverSocketPath, []grpc.DialOption{
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(cfg.Auth.Token)),
	}, handshaker)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return gitalypb.NewRepositoryServiceClient(conn)
}

func setupRepositoryServiceWithRuby(t testing.TB, cfg config.Cfg, rubySrv *rubyserver.Server, opts ...testserver.GitalyServerOpt) (config.Cfg, *gitalypb.Repository, string, gitalypb.RepositoryServiceClient) {
	client, serverSocketPath := runRepositoryService(t, cfg, rubySrv, opts...)
	testhelper.ConfigureGitalyGit2GoBin(t, cfg)
	cfg.SocketPath = serverSocketPath

	repo, repoPath, cleanup := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], t.Name())
	t.Cleanup(cleanup)

	return cfg, repo, repoPath, client
}

func assertModTimeAfter(t *testing.T, afterTime time.Time, paths ...string) bool {
	t.Helper()
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
		gitalypb.RegisterRepositoryServiceServer(srv, NewServer(
			cfg,
			deps.GetRubyServer(),
			deps.GetLocator(),
			deps.GetTxManager(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
		))
		gitalypb.RegisterHookServiceServer(srv, hookservice.NewServer(cfg, deps.GetHookManager(), deps.GetGitCmdFactory()))
		gitalypb.RegisterRemoteServiceServer(srv, remote.NewServer(
			cfg,
			rubySrv,
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
			deps.GetTxManager(),
		))
		gitalypb.RegisterSSHServiceServer(srv, ssh.NewServer(
			cfg,
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetTxManager(),
		))
		gitalypb.RegisterRefServiceServer(srv, ref.NewServer(
			cfg,
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetTxManager(),
			deps.GetCatfileCache(),
		))
		gitalypb.RegisterCommitServiceServer(srv, commit.NewServer(
			cfg,
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			nil,
			deps.GetCatfileCache(),
		))
	}, opts...)
}

func runRepositoryService(t testing.TB, cfg config.Cfg, rubySrv *rubyserver.Server, opts ...testserver.GitalyServerOpt) (gitalypb.RepositoryServiceClient, string) {
	serverSocketPath := runRepositoryServerWithConfig(t, cfg, rubySrv, opts...)
	client := newRepositoryClient(t, cfg, serverSocketPath)

	return client, serverSocketPath
}

func setupRepositoryService(t testing.TB, opts ...testserver.GitalyServerOpt) (config.Cfg, *gitalypb.Repository, string, gitalypb.RepositoryServiceClient) {
	cfg, client := setupRepositoryServiceWithoutRepo(t, opts...)
	repo, repoPath, cleanup := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], t.Name())
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

	repo, repoPath, cleanup := gittest.CloneRepoWithWorktreeAtStorage(t, cfg, cfg.Storages[0])
	t.Cleanup(cleanup)

	return cfg, repo, repoPath, client
}
