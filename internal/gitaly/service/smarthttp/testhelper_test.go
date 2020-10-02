package smarthttp

import (
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v13/auth"
	diskcache "gitlab.com/gitlab-org/gitaly/v13/internal/cache"
	"gitlab.com/gitlab-org/gitaly/v13/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v13/internal/middleware/cache"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	pktFlushStr = "0000"
)

func TestMain(m *testing.M) {
	testhelper.Configure()
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	hooks.Override = "/"

	cwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	config.Config.Ruby.Dir = filepath.Join(cwd, "../../../../ruby")

	testhelper.ConfigureGitalyHooksBinary()

	return m.Run()
}

func runSmartHTTPServer(t *testing.T, serverOpts ...ServerOpt) (string, func()) {
	keyer := diskcache.LeaseKeyer{}

	srv := testhelper.NewServer(t,
		[]grpc.StreamServerInterceptor{
			cache.StreamInvalidator(keyer, protoregistry.GitalyProtoPreregistered),
		},
		[]grpc.UnaryServerInterceptor{
			cache.UnaryInvalidator(keyer, protoregistry.GitalyProtoPreregistered),
		},
	)

	gitalypb.RegisterSmartHTTPServiceServer(srv.GrpcServer(), NewServer(serverOpts...))
	reflection.Register(srv.GrpcServer())

	require.NoError(t, srv.Start())

	return "unix://" + srv.Socket(), srv.Stop
}

func newSmartHTTPClient(t *testing.T, serverSocketPath string) (gitalypb.SmartHTTPServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(config.Config.Auth.Token)),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewSmartHTTPServiceClient(conn), conn
}
