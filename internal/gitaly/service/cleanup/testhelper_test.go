package cleanup

import (
	"os"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	hookservice "gitlab.com/gitlab-org/gitaly/internal/gitaly/service/hook"
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
	testhelper.ConfigureGitalyHooksBinary(config.Config.BinDir)
	return m.Run()
}

func runCleanupServiceServer(t *testing.T, cfg config.Cfg) (string, func()) {
	srv := testhelper.NewServer(t, nil, nil, testhelper.WithInternalSocket(cfg))

	locator := config.NewLocator(cfg)
	gitCmdFactory := git.NewExecCommandFactory(cfg)
	gitalypb.RegisterCleanupServiceServer(srv.GrpcServer(), NewServer(cfg, gitCmdFactory))
	gitalypb.RegisterHookServiceServer(
		srv.GrpcServer(),
		hookservice.NewServer(
			cfg,
			hook.NewManager(locator, transaction.NewManager(cfg), hook.GitlabAPIStub, cfg),
			gitCmdFactory,
		),
	)
	reflection.Register(srv.GrpcServer())

	srv.Start(t)

	return "unix://" + srv.Socket(), srv.Stop
}

func newCleanupServiceClient(t *testing.T, serverSocketPath string) (gitalypb.CleanupServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewCleanupServiceClient(conn), conn
}
