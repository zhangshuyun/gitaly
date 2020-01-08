package cleanup

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func runCleanupServiceServer(t *testing.T) (func(), string) {
	srv := testhelper.NewServer(t, nil, nil)

	gitalypb.RegisterCleanupServiceServer(srv.GrpcServer(), NewServer())
	reflection.Register(srv.GrpcServer())

	require.NoError(t, srv.Start())

	return srv.Stop, "unix://" + srv.Socket()
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
