package objectpool

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/internal/config"
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

	return m.Run()
}

func runObjectPoolServer(t *testing.T) (func(), string) {
	srv := testhelper.NewServer(t, nil, nil)

	gitalypb.RegisterObjectPoolServiceServer(srv.GrpcServer(), NewServer())
	reflection.Register(srv.GrpcServer())

	require.NoError(t, srv.Start())

	return srv.Stop, "unix://" + srv.Socket()
}

func newObjectPoolClient(t *testing.T, serverSocketPath string) (gitalypb.ObjectPoolServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials(config.Config.Auth.Token)),
	}

	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewObjectPoolServiceClient(conn), conn
}
