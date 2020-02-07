package repository

import (
	"crypto/x509"
	"log"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/client"
	dcache "gitlab.com/gitlab-org/gitaly/internal/cache"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	mcache "gitlab.com/gitlab-org/gitaly/internal/middleware/cache"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/server/auth"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
)

// Stamp taken from https://golang.org/pkg/time/#pkg-constants
const testTimeString = "200601021504.05"

var (
	testTime   = time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC)
	RubyServer = &rubyserver.Server{}
)

func newRepositoryClient(t *testing.T, serverSocketPath string) (gitalypb.RepositoryServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials(testhelper.RepositoryAuthToken)),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewRepositoryServiceClient(conn), conn
}

var NewRepositoryClient = newRepositoryClient
var RunRepoServer = runRepoServer

func newSecureRepoClient(t *testing.T, serverSocketPath string, pool *x509.CertPool) (gitalypb.RepositoryServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(pool, "")),
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials(testhelper.RepositoryAuthToken)),
	}

	conn, err := client.Dial(serverSocketPath, connOpts)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewRepositoryServiceClient(conn), conn
}

var NewSecureRepoClient = newSecureRepoClient

func runRepoServer(t *testing.T) (*grpc.Server, string) {
	streamInt := []grpc.StreamServerInterceptor{
		auth.StreamServerInterceptor(config.Config.Auth),
		mcache.StreamInvalidator(dcache.LeaseKeyer{}, protoregistry.GitalyProtoPreregistered),
	}
	unaryInt := []grpc.UnaryServerInterceptor{
		auth.UnaryServerInterceptor(config.Config.Auth),
		mcache.UnaryInvalidator(dcache.LeaseKeyer{}, protoregistry.GitalyProtoPreregistered),
	}

	server := testhelper.NewTestGrpcServer(t, streamInt, unaryInt)
	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName()

	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	gitalypb.RegisterRepositoryServiceServer(server, NewServer(RubyServer))
	reflection.Register(server)

	go server.Serve(listener)

	return server, "unix://" + serverSocketPath
}

func TestRepoNoAuth(t *testing.T) {
	srv, path := runRepoServer(t)
	defer srv.Stop()

	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}

	conn, err := grpc.Dial(path, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	client := gitalypb.NewRepositoryServiceClient(conn)
	_, err = client.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{Repository: &gitalypb.Repository{StorageName: "default", RelativePath: "new/project/path"}})

	testhelper.RequireGrpcError(t, err, codes.Unauthenticated)
}

func assertModTimeAfter(t *testing.T, afterTime time.Time, paths ...string) bool {
	// NOTE: Since some filesystems don't have sub-second precision on `mtime`
	//       we're rounding the times to seconds
	afterTime = afterTime.Round(time.Second)
	for _, path := range paths {
		s, err := os.Stat(path)
		assert.NoError(t, err)

		if !s.ModTime().Round(time.Second).After(afterTime) {
			t.Errorf("ModTime is not after afterTime: %q < %q", s.ModTime().Round(time.Second).String(), afterTime.String())
		}
	}
	return t.Failed()
}

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	config.Config.Auth.Token = testhelper.RepositoryAuthToken

	var err error
	config.Config.GitlabShell.Dir, err = filepath.Abs("testdata/gitlab-shell")
	if err != nil {
		log.Fatal(err)
	}

	testhelper.ConfigureGitalySSH()

	if err := RubyServer.Start(); err != nil {
		log.Fatal(err)
	}
	defer RubyServer.Stop()

	return m.Run()
}

const (
	ReplicationLockFileName = replicationLockFileName
)
