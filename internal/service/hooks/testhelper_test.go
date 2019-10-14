package hook

import (
	"net"
	"testing"

	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/server/auth"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func newHooksClient(t *testing.T, serverSocketPath string) (gitalypb.HookServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials(config.Config.Auth.Token)),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewHookServiceClient(conn), conn
}

func runHooksServer(t *testing.T) (*grpc.Server, string) {
	streamInt := []grpc.StreamServerInterceptor{auth.StreamServerInterceptor(config.Config.Auth)}
	unaryInt := []grpc.UnaryServerInterceptor{auth.UnaryServerInterceptor(config.Config.Auth)}

	server := testhelper.NewTestGrpcServer(t, streamInt, unaryInt)
	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName()

	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	gitalypb.RegisterHookServiceServer(server, NewServer())
	reflection.Register(server)

	go server.Serve(listener)

	return server, "unix://" + serverSocketPath
}
