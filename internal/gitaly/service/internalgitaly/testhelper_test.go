package internalgitaly

import (
	"net"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func runInternalGitalyServer(t *testing.T, srv gitalypb.InternalGitalyServer) (*grpc.Server, string) {
	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName()
	grpcServer := testhelper.NewTestGrpcServer(t, nil, nil)

	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	gitalypb.RegisterInternalGitalyServer(grpcServer, srv)
	reflection.Register(grpcServer)

	go grpcServer.Serve(listener)

	return grpcServer, "unix://" + serverSocketPath
}

func newInternalGitalyClient(t *testing.T, serverSocketPath string) (gitalypb.InternalGitalyClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewInternalGitalyClient(conn), conn
}
