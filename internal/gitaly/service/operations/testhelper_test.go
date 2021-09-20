package operations

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v14/auth"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	internalclient "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/commit"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/ssh"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitlab"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
)

var (
	gitlabPreHooks  = []string{"pre-receive", "update"}
	gitlabPostHooks = []string{"post-receive"}
	GitlabPreHooks  = gitlabPreHooks
	GitlabHooks     []string
)

func init() {
	GitlabHooks = append(GitlabHooks, append(gitlabPreHooks, gitlabPostHooks...)...)
}

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	cleanup := testhelper.Configure()
	defer cleanup()

	return m.Run()
}

func setupOperationsService(t testing.TB, ctx context.Context, options ...testserver.GitalyServerOpt) (context.Context, config.Cfg, *gitalypb.Repository, string, gitalypb.OperationServiceClient) {
	cfg := testcfg.Build(t)

	ctx, cfg, repo, repoPath, client := setupOperationsServiceWithRuby(t, ctx, cfg, nil, options...)

	return ctx, cfg, repo, repoPath, client
}

func setupOperationsServiceWithRuby(
	t testing.TB, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server, options ...testserver.GitalyServerOpt,
) (context.Context, config.Cfg, *gitalypb.Repository, string, gitalypb.OperationServiceClient) {
	repo, repoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])

	testhelper.BuildGitalySSH(t, cfg)
	testhelper.BuildGitalyGit2Go(t, cfg)
	testhelper.BuildGitalyHooks(t, cfg)

	serverSocketPath := runOperationServiceServer(t, cfg, rubySrv, options...)
	cfg.SocketPath = serverSocketPath

	client, conn := newOperationClient(t, serverSocketPath)
	t.Cleanup(func() { conn.Close() })

	md := testhelper.GitalyServersMetadataFromCfg(t, cfg)
	ctx = testhelper.MergeOutgoingMetadata(ctx, md)

	return ctx, cfg, repo, repoPath, client
}

func runOperationServiceServer(t testing.TB, cfg config.Cfg, rubySrv *rubyserver.Server, options ...testserver.GitalyServerOpt) string {
	t.Helper()

	return testserver.RunGitalyServer(t, cfg, rubySrv, func(srv *grpc.Server, deps *service.Dependencies) {
		operationServer := NewServer(
			deps.GetCfg(),
			deps.GetRubyServer(),
			deps.GetHookManager(),
			deps.GetTxManager(),
			deps.GetLocator(),
			deps.GetConnsPool(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
		)
		operationServer.enableUserMergeBranchStructuredErrors = true

		gitalypb.RegisterOperationServiceServer(srv, operationServer)
		gitalypb.RegisterHookServiceServer(srv, hook.NewServer(cfg, deps.GetHookManager(), deps.GetGitCmdFactory(), deps.GetPackObjectsCache()))
		gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(
			deps.GetCfg(),
			rubySrv,
			deps.GetLocator(),
			deps.GetTxManager(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
		))
		gitalypb.RegisterRefServiceServer(srv, ref.NewServer(
			deps.GetCfg(),
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetTxManager(),
			deps.GetCatfileCache(),
		))
		gitalypb.RegisterCommitServiceServer(srv, commit.NewServer(
			deps.GetCfg(),
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			nil,
			deps.GetCatfileCache(),
		))
		gitalypb.RegisterSSHServiceServer(srv, ssh.NewServer(
			deps.GetCfg(),
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetTxManager(),
		))
	}, options...)
}

func newOperationClient(t testing.TB, serverSocketPath string) (gitalypb.OperationServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewOperationServiceClient(conn), conn
}

func newMuxedOperationClient(t *testing.T, ctx context.Context, serverSocketPath, authToken string, handshaker internalclient.Handshaker) gitalypb.OperationServiceClient {
	conn, err := internalclient.Dial(ctx, serverSocketPath, []grpc.DialOption{grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(authToken))}, handshaker)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return gitalypb.NewOperationServiceClient(conn)
}

func setupAndStartGitlabServer(t testing.TB, glID, glRepository string, cfg config.Cfg, gitPushOptions ...string) string {
	url, cleanup := gitlab.SetupAndStartGitlabServer(t, cfg.GitlabShell.Dir, &gitlab.TestServerOptions{
		SecretToken:                 "secretToken",
		GLID:                        glID,
		GLRepository:                glRepository,
		PostReceiveCounterDecreased: true,
		Protocol:                    "web",
		GitPushOptions:              gitPushOptions,
	})

	t.Cleanup(cleanup)

	return url
}
