package ssh

import (
	"os"
	"path/filepath"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	hookservice "gitlab.com/gitlab-org/gitaly/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
)

var (
	gitalySSHPath string
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	defer func(rubyDir string) {
		config.Config.Ruby.Dir = rubyDir
	}(config.Config.Ruby.Dir)

	cleanup := testhelper.Configure()
	defer cleanup()

	testhelper.ConfigureGitalyHooksBinary()
	testhelper.ConfigureGitalySSH()
	gitalySSHPath = filepath.Join(config.Config.BinDir, "gitaly-ssh")

	return m.Run()
}

func runSSHServer(t *testing.T, serverOpts ...ServerOpt) (string, func()) {
	locator := config.NewLocator(config.Config)
	txManager := transaction.NewManager(config.Config)
	hookManager := hook.NewManager(locator, txManager, hook.GitlabAPIStub, config.Config)
	gitCmdFactory := git.NewExecCommandFactory(config.Config)

	srv := testhelper.NewServer(t, nil, nil, testhelper.WithInternalSocket(config.Config))
	gitalypb.RegisterSSHServiceServer(srv.GrpcServer(), NewServer(config.Config, locator, gitCmdFactory, serverOpts...))
	gitalypb.RegisterHookServiceServer(srv.GrpcServer(), hookservice.NewServer(config.Config, hookManager))
	srv.Start(t)

	return "unix://" + srv.Socket(), srv.Stop
}

func newSSHClient(t *testing.T, serverSocketPath string) (gitalypb.SSHServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewSSHServiceClient(conn), conn
}
