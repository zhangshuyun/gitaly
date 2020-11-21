package smarthttp

import (
	"os"
	"path/filepath"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	diskcache "gitlab.com/gitlab-org/gitaly/internal/cache"
	"gitlab.com/gitlab-org/gitaly/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/cache"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	pktFlushStr = "0000"
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	cleanup := testhelper.Configure()
	defer cleanup()

	hooks.Override = "/"

	cwd, err := os.Getwd()
	if err != nil {
		log.Error(err)
		return 1
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

	serverOpts = append([]ServerOpt{WithConfig(config.Config)}, serverOpts...)
	gitalypb.RegisterSmartHTTPServiceServer(srv.GrpcServer(), NewServer(config.NewLocator(config.Config), serverOpts...))
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
