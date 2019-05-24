package cleanup_test

import (
	"net"
	"testing"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"gitlab.com/gitlab-org/gitaly/internal/service/cleanup"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func runCleanupServiceServer(t *testing.T) (*grpc.Server, string) {
	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName()
	grpcServer := testhelper.NewTestGrpcServer(t, nil, nil)

	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	gitalypb.RegisterCleanupServiceServer(grpcServer, cleanup.NewServer())
	reflection.Register(grpcServer)

	go grpcServer.Serve(listener)

	return grpcServer, "unix://" + serverSocketPath
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
