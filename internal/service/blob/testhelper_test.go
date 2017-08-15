package blob

import (
	"net"
	"testing"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
)

var (
	testRepo         = testhelper.TestRepository()
	serverSocketPath = testhelper.GetTemporaryGitalySocketFileName()
)

func runBlobServer(t *testing.T) *grpc.Server {
	server := testhelper.NewTestGrpcServer(t, nil, nil)
	listener, err := net.Listen("unix", serverSocketPath)

	if err != nil {
		t.Fatal(err)
	}

	pb.RegisterBlobServiceServer(server, NewServer())
	reflection.Register(server)

	go server.Serve(listener)

	return server
}

func newBlobClient(t *testing.T, serverSocketPath string) (pb.BlobServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithDialer(func(addr string, _ time.Duration) (net.Conn, error) {
			return net.Dial("unix", addr)
		}),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return pb.NewBlobServiceClient(conn), conn
}
