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
	gconfig "gitlab.com/gitlab-org/gitaly/internal/config"
	internalauth "gitlab.com/gitlab-org/gitaly/internal/config/auth"
	"gitlab.com/gitlab-org/gitaly/internal/log"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/proxy"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/middleware"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/mock"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/transactions"
	"gitlab.com/gitlab-org/gitaly/internal/server/auth"
	"gitlab.com/gitlab-org/gitaly/internal/service/internalgitaly"
	"gitlab.com/gitlab-org/gitaly/internal/service/repository"
	gitalyserver "gitlab.com/gitlab-org/gitaly/internal/service/server"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/promtest"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	correlation "gitlab.com/gitlab-org/labkit/correlation/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
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
	var nodes []*models.Node

	for i := 0; i < backends; i++ {
		n := &models.Node{
			Storage: fmt.Sprintf("praefect-internal-%d", i),
			Token:   fmt.Sprintf("%d", i),
		}

		if i == 0 {
			n.DefaultPrimary = true
		}

		nodes = append(nodes, n)
	}
	cfg := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			&config.VirtualStorage{
				Name:  "praefect",
				Nodes: nodes,
			},
		},
	}

	return cfg
}

func assertPrimariesExist(t testing.TB, conf config.Config) {
	for _, vs := range conf.VirtualStorages {
		var defaultNode *models.Node
		for _, n := range vs.Nodes {
			if n.DefaultPrimary {
				defaultNode = n
			}
		}
		require.NotNil(t, defaultNode)
	}
}

// runPraefectServer runs a praefect server with the provided mock servers.
// Each mock server is keyed by the corresponding index of the node in the
// config.Nodes. There must be a 1-to-1 mapping between backend server and
// configured storage node.
// requires there to be only 1 virtual storage
func runPraefectServerWithMock(t *testing.T, conf config.Config, queue datastore.ReplicationEventQueue, backends map[string]mock.SimpleServiceServer) (*grpc.ClientConn, *Server, testhelper.Cleanup) {
	r, err := protoregistry.New(mustLoadProtoReg(t))
	require.NoError(t, err)

	return runPraefectServer(t, conf, buildOptions{
		withQueue:       queue,
		withBackends:    withMockBackends(t, backends),
		withAnnotations: r,
	})
}

func noopBackoffFunc() (backoff, backoffReset) {
	return func() time.Duration {
		return 0
	}, func() {}
}

type nullNodeMgr struct{}

func (nullNodeMgr) GetShard(virtualStorageName string) (nodes.Shard, error) {
	return nodes.Shard{}, nil
}

func (nullNodeMgr) EnableWrites(ctx context.Context, virtualStorageName string) error { return nil }
func (nullNodeMgr) GetSyncedNode(ctx context.Context, virtualStorageName, repoPath string) (nodes.Node, error) {
	return nil, nil
}

type buildOptions struct {
	withQueue       datastore.ReplicationEventQueue
	withTxMgr       *transactions.Manager
	withBackends    func([]*config.VirtualStorage) []testhelper.Cleanup
	withAnnotations *protoregistry.Registry
	withLogger      *logrus.Entry
	withNodeMgr     nodes.Manager
}

func withMockBackends(t testing.TB, backends map[string]mock.SimpleServiceServer) func([]*config.VirtualStorage) []testhelper.Cleanup {
	return func(virtualStorages []*config.VirtualStorage) []testhelper.Cleanup {
		var cleanups []testhelper.Cleanup

		for _, vs := range virtualStorages {
			require.Equal(t, len(backends), len(vs.Nodes),
				"mock server count doesn't match config nodes")

			for i, node := range vs.Nodes {
				backend, ok := backends[node.Storage]
				require.True(t, ok, "missing backend server for node %s", node.Storage)

				backendAddr, cleanup := newMockDownstream(t, node.Token, backend)
				cleanups = append(cleanups, cleanup)

				node.Address = backendAddr
				vs.Nodes[i] = node
			}
		}

		return cleanups
	}
}

func flattenVirtualStoragesToStoragePath(virtualStorages []*config.VirtualStorage, storagePath string) []gconfig.Storage {
	var storages []gconfig.Storage
	for _, vStorage := range virtualStorages {
		for _, node := range vStorage.Nodes {
			storages = append(storages, gconfig.Storage{
				Name: node.Storage,
				Path: storagePath,
			})
		}
	}
	return storages
}

// withRealGitalyShared will configure a real Gitaly server backend for a
// Praefect server. The same Gitaly server instance is used for all backend
// storages.
func withRealGitalyShared(t testing.TB) func([]*config.VirtualStorage) []testhelper.Cleanup {
	return func(virtualStorages []*config.VirtualStorage) []testhelper.Cleanup {
		gStorages := flattenVirtualStoragesToStoragePath(virtualStorages, testhelper.GitlabTestStoragePath())
		_, backendAddr, cleanupGitaly := runInternalGitalyServer(t, gStorages, virtualStorages[0].Nodes[0].Token)

		for _, vs := range virtualStorages {
			for i, node := range vs.Nodes {
				node.Address = backendAddr
				vs.Nodes[i] = node
			}
		}

		return []testhelper.Cleanup{cleanupGitaly}
	}
}

func runPraefectServerWithGitaly(t *testing.T, conf config.Config) (*grpc.ClientConn, *Server, testhelper.Cleanup) {
	return runPraefectServerWithGitalyWithDatastore(t, conf, defaultQueue(conf))
}

// runPraefectServerWithGitaly runs a praefect server with actual Gitaly nodes
// requires exactly 1 virtual storage
func runPraefectServerWithGitalyWithDatastore(t *testing.T, conf config.Config, queue datastore.ReplicationEventQueue) (*grpc.ClientConn, *Server, testhelper.Cleanup) {
	return runPraefectServer(t, conf, buildOptions{
		withQueue:    queue,
		withTxMgr:    transactions.NewManager(),
		withBackends: withRealGitalyShared(t),
	})
}

func defaultQueue(conf config.Config) datastore.ReplicationEventQueue {
	return datastore.NewMemoryReplicationEventQueue(conf)
}

func defaultTxMgr() *transactions.Manager {
	return transactions.NewManager()
}

func defaultNodeMgr(t testing.TB, conf config.Config, queue datastore.ReplicationEventQueue) nodes.Manager {
	nodeMgr, err := nodes.NewManager(testhelper.DiscardTestEntry(t), conf, nil, queue, middleware.NewErrors(0, 0, 0), promtest.NewMockHistogramVec())
	require.NoError(t, err)
	nodeMgr.Start(1*time.Millisecond, 5*time.Millisecond)
	return nodeMgr
}

func runPraefectServer(t testing.TB, conf config.Config, opt buildOptions) (*grpc.ClientConn, *Server, testhelper.Cleanup) {
	assertPrimariesExist(t, conf)

	var cleanups []testhelper.Cleanup

	if opt.withQueue == nil {
		opt.withQueue = defaultQueue(conf)
	}
	if opt.withTxMgr == nil {
		opt.withTxMgr = defaultTxMgr()
	}
	if opt.withBackends != nil {
		cleanups = append(cleanups, opt.withBackends(conf.VirtualStorages)...)
	}
	if opt.withAnnotations == nil {
		opt.withAnnotations = protoregistry.GitalyProtoPreregistered
	}
	if opt.withLogger == nil {
		opt.withLogger = log.Default()
	}
	if opt.withNodeMgr == nil {
		opt.withNodeMgr = defaultNodeMgr(t, conf, opt.withQueue)
	}

	coordinator := NewCoordinator(
		opt.withQueue,
		opt.withNodeMgr,
		opt.withTxMgr,
		conf,
		opt.withAnnotations,
	)

	// TODO: run a replmgr for EVERY virtual storage
	replmgr := NewReplMgr(
		opt.withLogger,
		conf.VirtualStorageNames(),
		opt.withQueue,
		opt.withNodeMgr,
		WithQueueMetric(&promtest.MockGauge{}),
	)
	prf := NewServer(coordinator.StreamDirector, opt.withLogger, opt.withAnnotations, conf)

	listener, port := listenAvailPort(t)
	t.Logf("proxy listening on port %d", port)

	errQ := make(chan error)
	ctx, cancel := testhelper.Context()

	prf.RegisterServices(opt.withNodeMgr, opt.withTxMgr, conf, opt.withQueue)
	go func() { errQ <- prf.Serve(listener, false) }()
	replmgr.ProcessBacklog(ctx, noopBackoffFunc)

	// dial client to praefect
	cc := dialLocalPort(t, port, false)

	cleanup := func() {
		for _, cu := range cleanups {
			cu()
		}

		ctx, timed := context.WithTimeout(ctx, time.Second)
		defer timed()
		require.NoError(t, prf.Shutdown(ctx))

		cancel()
		require.Error(t, context.Canceled, <-errQ)
	}

	return cc, prf, cleanup
}

// partialGitaly is a subset of Gitaly's behavior needed to properly test
// Praefect
type partialGitaly interface {
	gitalypb.ServerServiceServer
	gitalypb.RepositoryServiceServer
	gitalypb.InternalGitalyServer
	healthpb.HealthServer
}

func registerServices(server *grpc.Server, pg partialGitaly) {
	gitalypb.RegisterServerServiceServer(server, pg)
	gitalypb.RegisterRepositoryServiceServer(server, pg)
	gitalypb.RegisterInternalGitalyServer(server, pg)
	healthpb.RegisterHealthServer(server, pg)
}

func realGitaly(storages []gconfig.Storage, authToken, internalSocketPath string) partialGitaly {
	return struct {
		gitalypb.ServerServiceServer
		gitalypb.RepositoryServiceServer
		gitalypb.InternalGitalyServer
		healthpb.HealthServer
	}{
		gitalyserver.NewServer(storages),
		repository.NewServer(RubyServer, internalSocketPath),
		internalgitaly.NewServer(gconfig.Config.Storages),
		health.NewServer(),
	}
}

func runInternalGitalyServer(t testing.TB, storages []gconfig.Storage, token string) (*grpc.Server, string, func()) {
	streamInt := []grpc.StreamServerInterceptor{auth.StreamServerInterceptor(internalauth.Config{Token: token})}
	unaryInt := []grpc.UnaryServerInterceptor{auth.UnaryServerInterceptor(internalauth.Config{Token: token})}

	server := testhelper.NewTestGrpcServer(t, streamInt, unaryInt)
	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName()

	listener, err := net.Listen("unix", serverSocketPath)
	require.NoError(t, err)

	internalSocketPath := gconfig.GitalyInternalSocketPath()
	internalListener, err := net.Listen("unix", internalSocketPath)
	require.NoError(t, err)

	registerServices(server, realGitaly(storages, token, internalSocketPath))

	errQ := make(chan error)

	go func() { errQ <- server.Serve(listener) }()
	go func() { errQ <- server.Serve(internalListener) }()

	cleanup := func() {
		server.Stop()
		require.NoError(t, <-errQ)
		require.NoError(t, <-errQ)
	}

	return server, "unix://" + serverSocketPath, cleanup
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
		grpc.WithUnaryInterceptor(correlation.UnaryClientCorrelationInterceptor()),
		grpc.WithStreamInterceptor(correlation.StreamClientCorrelationInterceptor()),
	}
	if backend {
		opts = append(
			opts,
			grpc.WithDefaultCallOptions(grpc.ForceCodec(proxy.NewCodec())),
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
