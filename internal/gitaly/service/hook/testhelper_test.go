package hook

import (
	"os"
	"testing"

	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	gitalyhook "gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()
	cleanup := testhelper.Configure()
	defer cleanup()
	return m.Run()
}

func newHooksClient(t *testing.T, serverSocketPath string) (gitalypb.HookServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(config.Config.Auth.Token)),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewHookServiceClient(conn), conn
}

func runHooksServer(t *testing.T, cfg config.Cfg) (string, func()) {
	return runHooksServerWithAPI(t, gitalyhook.GitlabAPIStub, cfg)
}

func runHooksServerWithAPI(t *testing.T, gitlabAPI gitalyhook.GitlabAPI, cfg config.Cfg) (string, func()) {
	srv := testhelper.NewServer(t, nil, nil)

	gitalypb.RegisterHookServiceServer(srv.GrpcServer(), NewServer(cfg, gitalyhook.NewManager(config.NewLocator(cfg), transaction.NewManager(cfg), gitlabAPI, cfg)))
	reflection.Register(srv.GrpcServer())

	srv.Start(t)

	return "unix://" + srv.Socket(), srv.Stop
}
