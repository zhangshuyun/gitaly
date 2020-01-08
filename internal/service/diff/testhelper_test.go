package diff

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
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	return m.Run()
}

func runDiffServer(t *testing.T) (func(), string) {
	srv := testhelper.NewServer(t, nil, nil)

	gitalypb.RegisterDiffServiceServer(srv.GrpcServer(), NewServer())
	reflection.Register(srv.GrpcServer())

	require.NoError(t, srv.Start())

	return srv.Stop, "unix://" + srv.Socket()
}

func newDiffClient(t *testing.T, serverSocketPath string) (gitalypb.DiffServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}

	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewDiffServiceClient(conn), conn
}
