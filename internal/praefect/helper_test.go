package praefect

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/client"
	internalauth "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config/auth"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/server/auth"
	"gitlab.com/gitlab-org/gitaly/v14/internal/log"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/grpc-proxy/proxy"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/mock"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/transactions"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/promtest"
	correlation "gitlab.com/gitlab-org/labkit/correlation/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// generates a praefect configuration with the specified number of backend
// nodes
func testConfig(backends int) config.Config {
	var nodes []*config.Node

	for i := 0; i < backends; i++ {
		n := &config.Node{
			Storage: fmt.Sprintf("praefect-internal-%d", i),
			Token:   fmt.Sprintf("%d", i),
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

func noopBackoffFunc() (backoff, backoffReset) {
	return func() time.Duration {
		return 0
	}, func() {}
}

type nullNodeMgr struct{}

func (nullNodeMgr) GetShard(ctx context.Context, virtualStorageName string) (nodes.Shard, error) {
	return nodes.Shard{Primary: &nodes.MockNode{}}, nil
}

func (nullNodeMgr) GetSyncedNode(ctx context.Context, virtualStorageName, repoPath string) (nodes.Node, error) {
	return nil, nil
}

func (nullNodeMgr) HealthyNodes() map[string][]string {
	return nil
}

func (nullNodeMgr) Nodes() map[string][]nodes.Node {
	return nil
}

type buildOptions struct {
	withQueue           datastore.ReplicationEventQueue
	withTxMgr           *transactions.Manager
	withBackends        func([]*config.VirtualStorage) []testhelper.Cleanup
	withAnnotations     *protoregistry.Registry
	withLogger          *logrus.Entry
	withNodeMgr         nodes.Manager
	withRepoStore       datastore.RepositoryStore
	withAssignmentStore AssignmentStore
	withConnections     Connections
	withPrimaryGetter   PrimaryGetter
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

func defaultQueue(conf config.Config) datastore.ReplicationEventQueue {
	return datastore.NewMemoryReplicationEventQueue(conf)
}

func defaultTxMgr(conf config.Config) *transactions.Manager {
	return transactions.NewManager(conf)
}

func defaultNodeMgr(t testing.TB, conf config.Config, rs datastore.RepositoryStore) nodes.Manager {
	nodeMgr, err := nodes.NewManager(testhelper.DiscardTestEntry(t), conf, nil, rs, promtest.NewMockHistogramVec(), protoregistry.GitalyProtoPreregistered, nil, nil)
	require.NoError(t, err)
	nodeMgr.Start(0, time.Hour)
	return nodeMgr
}

func defaultRepoStore(conf config.Config) datastore.RepositoryStore {
	return datastore.MockRepositoryStore{}
}

func runPraefectServer(t testing.TB, conf config.Config, opt buildOptions) (*grpc.ClientConn, *grpc.Server, testhelper.Cleanup) {
	var cleanups []testhelper.Cleanup

	if opt.withQueue == nil {
		opt.withQueue = defaultQueue(conf)
	}
	if opt.withRepoStore == nil {
		opt.withRepoStore = defaultRepoStore(conf)
	}
	if opt.withTxMgr == nil {
		opt.withTxMgr = defaultTxMgr(conf)
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
		opt.withNodeMgr = defaultNodeMgr(t, conf, opt.withRepoStore)
	}
	if opt.withAssignmentStore == nil {
		opt.withAssignmentStore = NewDisabledAssignmentStore(conf.StorageNames())
	}

	coordinator := NewCoordinator(
		opt.withQueue,
		opt.withRepoStore,
		NewNodeManagerRouter(opt.withNodeMgr, opt.withRepoStore),
		opt.withTxMgr,
		conf,
		opt.withAnnotations,
	)

	// TODO: run a replmgr for EVERY virtual storage
	replmgr := NewReplMgr(
		opt.withLogger,
		conf.VirtualStorageNames(),
		opt.withQueue,
		opt.withRepoStore,
		opt.withNodeMgr,
		NodeSetFromNodeManager(opt.withNodeMgr),
	)

	prf := NewGRPCServer(
		conf,
		opt.withLogger,
		protoregistry.GitalyProtoPreregistered,
		coordinator.StreamDirector,
		opt.withNodeMgr,
		opt.withTxMgr,
		opt.withQueue,
		opt.withRepoStore,
		opt.withAssignmentStore,
		opt.withConnections,
		opt.withPrimaryGetter,
	)

	listener, port := listenAvailPort(t)

	errQ := make(chan error)
	ctx, cancel := testhelper.Context()

	go func() { errQ <- prf.Serve(listener) }()
	go replmgr.ProcessBacklog(ctx, noopBackoffFunc)

	// dial client to praefect
	cc := dialLocalPort(t, port, false)

	cleanup := func() {
		cc.Close()

		for _, cu := range cleanups {
			cu()
		}

		prf.Stop()

		cancel()
		require.NoError(t, <-errQ)
	}

	return cc, prf, cleanup
}

func mustLoadProtoReg(t testing.TB) *descriptor.FileDescriptorProto {
	fd, err := protoregistry.ExtractFileDescriptor(proto.FileDescriptor("praefect/mock/mock.proto"))
	require.NoError(t, err)
	return fd
}

func listenAvailPort(tb testing.TB) (net.Listener, int) {
	listener, err := net.Listen("tcp", "localhost:0")
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
	healthpb.RegisterHealthServer(srv, health.NewServer())

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
