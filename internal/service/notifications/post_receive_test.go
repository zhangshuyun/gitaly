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

func TestSuccessfulPostReceive(t *testing.T) {
	server, serverSocketPath := runNotificationsServer(t)
	defer server.Stop()

	client, conn := newNotificationsClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	rpcRequest := &pb.PostReceiveRequest{Repository: testRepo}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := client.PostReceive(ctx, rpcRequest)
	if err != nil {
		t.Fatal(err)
	}
}

func TestEmptyPostReceiveRequest(t *testing.T) {
	server, serverSocketPath := runNotificationsServer(t)
	defer server.Stop()

	client, conn := newNotificationsClient(t, serverSocketPath)
	defer conn.Close()
	rpcRequest := &pb.PostReceiveRequest{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := client.PostReceive(ctx, rpcRequest)
	testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
}

func runNotificationsServer(t *testing.T) (*grpc.Server, string) {
	server := testhelper.NewTestGrpcServer(t, nil, nil)

	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName()
	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	pb.RegisterNotificationsServer(server, NewServer())
	reflection.Register(server)

	go server.Serve(listener)

	return server, serverSocketPath
}

func newNotificationsClient(t *testing.T, serverSocketPath string) (pb.NotificationsClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
			return net.DialTimeout("unix", addr, timeout)
		}),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return pb.NewNotificationsClient(conn), conn
}
