package operations

import (
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
	RubyServer      = &rubyserver.Server{}
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

	testhelper.ConfigureGitalySSH()
	testhelper.ConfigureGitalyGit2Go()
	testhelper.ConfigureGitalyHooksBinary()

	defer func(token string) {
		config.Config.Auth.Token = token
	}(config.Config.Auth.Token)
	config.Config.Auth.Token = testhelper.RepositoryAuthToken

	if err := RubyServer.Start(); err != nil {
		log.Error(err)
		return 1
	}
	defer RubyServer.Stop()

	return m.Run()
}

func runOperationServiceServer(t *testing.T) (string, func()) {
	return runOperationServiceServerWithRubyServer(t, RubyServer)
}

func runOperationServiceServerWithRubyServer(t *testing.T, ruby *rubyserver.Server) (string, func()) {
	srv := testhelper.NewServerWithAuth(t, nil, nil, config.Config.Auth.Token, testhelper.WithInternalSocket(config.Config))

	conns := client.NewPool()

	locator := config.NewLocator(config.Config)
	hookManager := gitalyhook.NewManager(locator, gitalyhook.GitlabAPIStub, config.Config)
	server := NewServer(config.Config, ruby, hookManager, locator, conns, git.NewExecCommandFactory(config.Config))

	gitalypb.RegisterOperationServiceServer(srv.GrpcServer(), server)
	gitalypb.RegisterHookServiceServer(srv.GrpcServer(), hook.NewServer(config.Config, hookManager))
	gitalypb.RegisterRepositoryServiceServer(srv.GrpcServer(), repository.NewServer(config.Config, ruby, locator, git.NewExecCommandFactory(config.Config)))
	gitalypb.RegisterRefServiceServer(srv.GrpcServer(), ref.NewServer(config.Config, locator))
	gitalypb.RegisterCommitServiceServer(srv.GrpcServer(), commit.NewServer(config.Config, locator))
	gitalypb.RegisterSSHServiceServer(srv.GrpcServer(), ssh.NewServer(locator))
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

func setupAndStartGitlabServer(t testing.TB, glID, glRepository string, gitPushOptions ...string) func() {
	url, cleanup := testhelper.SetupAndStartGitlabServer(t, &testhelper.GitlabTestServerOptions{
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
