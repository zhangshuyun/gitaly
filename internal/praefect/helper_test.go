package praefect

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/client"
	internalauth "gitlab.com/gitlab-org/gitaly/internal/config/auth"
	"gitlab.com/gitlab-org/gitaly/internal/log"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/conn"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/proxy"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/mock"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/internal/server/auth"
	gitalyserver "gitlab.com/gitlab-org/gitaly/internal/service/server"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func waitUntil(t *testing.T, ch <-chan struct{}, timeout time.Duration) {
	select {
	case <-ch:
		break
	case <-time.After(timeout):
		t.Errorf("timed out waiting for channel after %s", timeout)
	}
}

// generates a praefect configuration with the specified number of backend
// nodes
func testConfig(backends int) config.Config {
	cfg := config.Config{
		VirtualStorageName: "praefect",
	}

	var nodes []*models.Node

	for i := 0; i < backends; i++ {
		n := &models.Node{
			ID:      i,
			Storage: fmt.Sprintf("praefect-internal-%d", i),
			Token:   fmt.Sprintf("%d", i),
		}

		if i == 0 {
			n.DefaultPrimary = true
		}

		nodes = append(nodes, n)
	}

	cfg.Nodes = nodes

	return cfg
}

// setupServer wires all praefect dependencies together via dependency
// injection
func setupServer(t testing.TB, conf config.Config, l *logrus.Entry, fds []*descriptor.FileDescriptorProto) (*MemoryDatastore, *conn.ClientConnections, *Server) {
	var (
		datastore   = NewMemoryDatastore(conf)
		clientCC    = conn.NewClientConnections()
		coordinator = NewCoordinator(l, datastore, clientCC, conf, fds...)
	)

	var defaultNode *models.Node
	for _, n := range conf.Nodes {
		if n.DefaultPrimary {
			defaultNode = n
		}
	}
	require.NotNil(t, defaultNode)

	replmgr := NewReplMgr(
		defaultNode.Storage,
		l,
		datastore,
		clientCC,
	)
	server := NewServer(
		coordinator,
		replmgr,
		nil,
		l,
		clientCC,
		conf,
	)

	return datastore, clientCC, server
}

// runPraefectServer runs a praefect server with the provided mock servers.
// Each mock server is keyed by the corresponding index of the node in the
// config.Nodes. There must be a 1-to-1 mapping between backend server and
// configured storage node.
func runPraefectServerWithMock(t *testing.T, conf config.Config, backends map[int]mock.SimpleServiceServer) (mock.SimpleServiceClient, *Server, func()) {
	datastore, clientCC, prf := setupServer(t, conf, log.Default(), []*descriptor.FileDescriptorProto{mustLoadProtoReg(t)})

	require.Equal(t, len(backends), len(conf.Nodes),
		"mock server count doesn't match config nodes")

	var cleanups []func()

	for id, nodeStorage := range datastore.storageNodes.m {
		backend, ok := backends[id]
		require.True(t, ok, "missing backend server for node %d", id)

		backendAddr, cleanup := newMockDownstream(t, nodeStorage.Token, backend)
		cleanups = append(cleanups, cleanup)

		clientCC.RegisterNode(nodeStorage.Storage, backendAddr, nodeStorage.Token)
		nodeStorage.Address = backendAddr
		datastore.storageNodes.m[id] = nodeStorage
	}

	listener, port := listenAvailPort(t)
	t.Logf("praefect listening on port %d", port)

	errQ := make(chan error)

	go func() {
		errQ <- prf.Start(listener)
	}()

	// dial client to praefect
	cc := dialLocalPort(t, port, false)

	cleanup := func() {
		for _, cu := range cleanups {
			cu()
		}
		require.NoError(t, cc.Close())
		require.NoError(t, listener.Close())

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		require.NoError(t, prf.Shutdown(ctx))
	}

	return mock.NewSimpleServiceClient(cc), prf, cleanup
}

// runPraefectServerWithGitaly runs a praefect server with actual Gitaly nodes
func runPraefectServerWithGitaly(t *testing.T, conf config.Config) (*grpc.ClientConn, *Server) {
	datastore := NewMemoryDatastore(conf)
	logEntry := log.Default()
	clientCC := conn.NewClientConnections()

	for id, nodeStorage := range datastore.storageNodes.m {
		_, backend := runInternalGitalyServer(t, nodeStorage.Token)

		clientCC.RegisterNode(nodeStorage.Storage, backend, nodeStorage.Token)
		nodeStorage.Address = backend
		datastore.storageNodes.m[id] = nodeStorage
	}

	coordinator := NewCoordinator(logEntry, datastore, clientCC, conf, protoregistry.GitalyProtoFileDescriptors...)

	replmgr := NewReplMgr(
		"",
		logEntry,
		datastore,
		clientCC,
	)

	prf := NewServer(
		coordinator,
		replmgr,
		nil,
		logEntry,
		clientCC,
		conf,
	)

	listener, port := listenAvailPort(t)
	t.Logf("proxy listening on port %d", port)

	errQ := make(chan error)

	go func() {
		errQ <- prf.Start(listener)
	}()

	// dial client to praefect
	cc := dialLocalPort(t, port, false)

	return cc, prf
}

func runInternalGitalyServer(t *testing.T, token string) (*grpc.Server, string) {
	streamInt := []grpc.StreamServerInterceptor{auth.StreamServerInterceptor(internalauth.Config{Token: token})}
	unaryInt := []grpc.UnaryServerInterceptor{auth.UnaryServerInterceptor(internalauth.Config{Token: token})}

	server := testhelper.NewTestGrpcServer(t, streamInt, unaryInt)
	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName()

	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	gitalypb.RegisterServerServiceServer(server, gitalyserver.NewServer())

	go server.Serve(listener)

	return server, "unix://" + serverSocketPath
}

func mustLoadProtoReg(t testing.TB) *descriptor.FileDescriptorProto {
	gz, _ := (*mock.SimpleRequest)(nil).Descriptor()
	fd, err := protoregistry.ExtractFileDescriptor(gz)
	require.NoError(t, err)
	return fd
}

func listenAvailPort(tb testing.TB) (net.Listener, int) {
	listener, err := net.Listen("tcp", ":0")
	require.NoError(tb, err)

	return listener, listener.Addr().(*net.TCPAddr).Port
}

func dialLocalPort(tb testing.TB, port int, backend bool) *grpc.ClientConn {
	opts := []grpc.DialOption{
		grpc.WithBlock(),
	}
	if backend {
		opts = append(
			opts,
			grpc.WithDefaultCallOptions(grpc.CallCustomCodec(proxy.Codec())),
		)
	}

	cc, err := client.Dial(
		fmt.Sprintf("tcp://localhost:%d", port),
		opts,
	)
	require.NoError(tb, err)

	return cc
}

func newMockDownstream(tb testing.TB, token string, m mock.SimpleServiceServer) (string, func()) {
	srv := grpc.NewServer(grpc.UnaryInterceptor(auth.UnaryServerInterceptor(internalauth.Config{Token: token})))
	mock.RegisterSimpleServiceServer(srv, m)

	// client to backend service
	lis, port := listenAvailPort(tb)

	errQ := make(chan error)

	go func() {
		errQ <- srv.Serve(lis)
	}()

	cleanup := func() {
		srv.GracefulStop()
		lis.Close()

		// If the server is shutdown before Serve() is called on it
		// the Serve() calls will return the ErrServerStopped
		if err := <-errQ; err != nil && err != grpc.ErrServerStopped {
			require.NoError(tb, err)
		}
	}

	return fmt.Sprintf("tcp://localhost:%d", port), cleanup
}
