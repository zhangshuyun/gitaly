package hook

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v13/auth"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/config"
	gitalyhook "gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func TestMain(m *testing.M) {
	testhelper.Configure()
	os.Exit(m.Run())
}

func newHooksClient(t *testing.T, serverSocketPath string) (gitalypb.HookServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(config.Config.Auth.Token)),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewHookServiceClient(conn), conn
}

func runHooksServer(t *testing.T, cfg config.Cfg) (string, func()) {
	return runHooksServerWithAPI(t, gitalyhook.GitlabAPIStub, cfg)
}

func runHooksServerWithAPI(t *testing.T, gitlabAPI gitalyhook.GitlabAPI, cfg config.Cfg) (string, func()) {
	srv := testhelper.NewServer(t, nil, nil)

	gitalypb.RegisterHookServiceServer(srv.GrpcServer(), NewServer(gitalyhook.NewManager(gitlabAPI, cfg)))
	reflection.Register(srv.GrpcServer())

	require.NoError(t, srv.Start())

	return "unix://" + srv.Socket(), srv.Stop
}
