package notifications

import (
	"context"
	"net"
	"testing"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
)

var (
	serverSocketPath = testhelper.GetTemporaryGitalySocketFileName()
	testRepo         = testhelper.TestRepository()
)

func TestSuccessfulPostReceive(t *testing.T) {
	server := runNotificationsServer(t)
	defer server.Stop()

	client, conn := newNotificationsClient(t)
	defer conn.Close()
	rpcRequest := &pb.PostReceiveRequest{Repository: testRepo}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := client.PostReceive(ctx, rpcRequest)
	if err != nil {
		t.Fatal(err)
	}
}

func TestEmptyPostReceiveRequest(t *testing.T) {
	server := runNotificationsServer(t)
	defer server.Stop()

	client, conn := newNotificationsClient(t)
	defer conn.Close()
	rpcRequest := &pb.PostReceiveRequest{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := client.PostReceive(ctx, rpcRequest)
	testhelper.AssertGrpcError(t, err, codes.InvalidArgument, "")
}

func runNotificationsServer(t *testing.T) *grpc.Server {
	server := testhelper.NewTestGrpcServer(t, nil, nil)
	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	pb.RegisterNotificationsServer(server, NewServer())
	reflection.Register(server)

	go server.Serve(listener)

	return server
}

func newNotificationsClient(t *testing.T) (pb.NotificationsClient, *grpc.ClientConn) {
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

	return pb.NewNotificationsClient(conn), conn
}
