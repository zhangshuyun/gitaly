package operations

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	log "github.com/sirupsen/logrus"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	gitalyhook "gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/commit"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/ssh"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	gitlabPreHooks  = []string{"pre-receive", "update"}
	gitlabPostHooks = []string{"post-receive"}
	GitlabPreHooks  = gitlabPreHooks
	GitlabHooks     []string
	RubyServer      *rubyserver.Server
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

	gitlabShellDir, err := ioutil.TempDir("", "gitlab-shell")
	if err != nil {
		log.Error(err)
		return 1
	}
	defer os.RemoveAll(gitlabShellDir)

	config.Config.GitlabShell.Dir = gitlabShellDir

	testhelper.ConfigureGitalySSH(config.Config.BinDir)
	testhelper.ConfigureGitalyGit2Go(config.Config.BinDir)
	testhelper.ConfigureGitalyHooksBinary(config.Config.BinDir)

	defer func(token string) {
		config.Config.Auth.Token = token
	}(config.Config.Auth.Token)
	config.Config.Auth.Token = testhelper.RepositoryAuthToken

	RubyServer = rubyserver.New(config.Config)
	if err := RubyServer.Start(); err != nil {
		log.Error(err)
		return 1
	}
	defer RubyServer.Stop()

	return m.Run()
}

func runOperationServiceServer(t *testing.T) (string, func()) {
	srv := testhelper.NewServerWithAuth(t, nil, nil, config.Config.Auth.Token, testhelper.WithInternalSocket(config.Config))

	conns := client.NewPool()

	locator := config.NewLocator(config.Config)
	txManager := transaction.NewManager(config.Config)
	hookManager := gitalyhook.NewManager(locator, txManager, gitalyhook.GitlabAPIStub, config.Config)
	gitCmdFactory := git.NewExecCommandFactory(config.Config)
	server := NewServer(config.Config, RubyServer, hookManager, locator, conns, gitCmdFactory)

	gitalypb.RegisterOperationServiceServer(srv.GrpcServer(), server)
	gitalypb.RegisterHookServiceServer(srv.GrpcServer(), hook.NewServer(config.Config, hookManager, gitCmdFactory))
	gitalypb.RegisterRepositoryServiceServer(srv.GrpcServer(), repository.NewServer(config.Config, RubyServer, locator, txManager, gitCmdFactory))
	gitalypb.RegisterRefServiceServer(srv.GrpcServer(), ref.NewServer(config.Config, locator, gitCmdFactory))
	gitalypb.RegisterCommitServiceServer(srv.GrpcServer(), commit.NewServer(config.Config, locator, gitCmdFactory, nil))
	gitalypb.RegisterSSHServiceServer(srv.GrpcServer(), ssh.NewServer(config.Config, locator, gitCmdFactory))
	reflection.Register(srv.GrpcServer())

	srv.Start(t)

	return "unix://" + srv.Socket(), srv.Stop
}

func newOperationClient(t *testing.T, serverSocketPath string) (gitalypb.OperationServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(config.Config.Auth.Token)),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewOperationServiceClient(conn), conn
}

func setupOperationClient(t *testing.T, ctx context.Context) (gitalypb.OperationServiceClient, context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	t.Cleanup(stop)

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx = testhelper.MergeOutgoingMetadata(ctx, md)

	client, conn := newOperationClient(t, serverSocketPath)
	t.Cleanup(func() { conn.Close() })

	return client, ctx
}

func setupAndStartGitlabServer(t testing.TB, glID, glRepository string, gitPushOptions ...string) func() {
	url, cleanup := testhelper.SetupAndStartGitlabServer(t, config.Config.GitlabShell.Dir, &testhelper.GitlabTestServerOptions{
		SecretToken:                 "secretToken",
		GLID:                        glID,
		GLRepository:                glRepository,
		PostReceiveCounterDecreased: true,
		Protocol:                    "web",
		GitPushOptions:              gitPushOptions,
	})

	gitlabURL := config.Config.Gitlab.URL
	cleanupAll := func() {
		cleanup()
		config.Config.Gitlab.URL = gitlabURL
	}
	config.Config.Gitlab.URL = url

	return cleanupAll
}
