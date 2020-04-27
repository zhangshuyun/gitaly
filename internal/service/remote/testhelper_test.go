package remote

import (
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var RubyServer = &rubyserver.Server{}

func TestMain(m *testing.M) {
	testhelper.Configure()
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	testhelper.ConfigureGitalySSH()

	if err := RubyServer.Start(); err != nil {
		log.Fatal(err)
	}
	defer RubyServer.Stop()

	return m.Run()
}

func runRemoteServiceServer(t *testing.T, storages config.Storages) (string, func()) {
	srv := testhelper.NewServer(t, nil, nil)

	gitalypb.RegisterRemoteServiceServer(srv.GrpcServer(), NewServer(RubyServer, storages))
	reflection.Register(srv.GrpcServer())

	require.NoError(t, srv.Start())

	return "unix://" + srv.Socket(), srv.Stop
}

func NewRemoteClient(t *testing.T, serverSocketPath string) (gitalypb.RemoteServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewRemoteServiceClient(conn), conn
}
