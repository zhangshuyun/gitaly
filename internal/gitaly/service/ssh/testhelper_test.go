package ssh

import (
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	gitalyhook "gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
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

	config.Config.Ruby.Dir = filepath.Join("../../../ruby", "git-hooks")
	testhelper.ConfigureGitalyHooksBinary()
	testhelper.ConfigureGitalySSH()
	gitalySSHPath = filepath.Join(config.Config.BinDir, "gitaly-ssh")

	return m.Run()
}

func runSSHServer(t *testing.T, serverOpts ...ServerOpt) (string, func()) {
	srv := testhelper.NewServer(t, nil, nil)

	serverOpts = append([]ServerOpt{WithConfig(config.Config)}, serverOpts...)
	gitalypb.RegisterSSHServiceServer(srv.GrpcServer(), NewServer(config.NewLocator(config.Config), serverOpts...))
	gitalypb.RegisterHookServiceServer(srv.GrpcServer(), hook.NewServer(gitalyhook.NewManager(gitalyhook.GitlabAPIStub, config.Config)))
	reflection.Register(srv.GrpcServer())

	internalListener, err := net.Listen("unix", config.Config.GitalyInternalSocketPath())
	if err != nil {
		t.Fatal(err)
	}

	require.NoError(t, srv.Start())
	go srv.GrpcServer().Serve(internalListener)

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
