package objectpool

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func TestMain(m *testing.M) {
	testhelper.Configure()
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	return m.Run()
}

func runObjectPoolServer(t *testing.T) (string, func()) {
	server := testhelper.NewServer(t, nil, nil)

	gitalypb.RegisterObjectPoolServiceServer(server.GrpcServer(), NewServer())
	reflection.Register(server.GrpcServer())

	require.NoError(t, server.Start())

	return "unix://" + server.Socket(), server.Stop
}

func newObjectPoolClient(t *testing.T, serverSocketPath string) (gitalypb.ObjectPoolServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}

	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewObjectPoolServiceClient(conn), conn
}
