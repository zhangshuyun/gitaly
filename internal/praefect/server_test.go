package praefect

import (
	"bytes"
	"context"
	"crypto/sha1"
	"errors"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	gconfig "gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/proxy"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metadata"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/mock"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes/tracker"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/transactions"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/promtest"
	"gitlab.com/gitlab-org/gitaly/internal/version"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	grpc_metadata "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestGitalyServerInfo(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	gitVersion, err := git.Version(ctx)
	require.NoError(t, err)

	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			&config.VirtualStorage{
				Name: "virtual-storage-1",
				Nodes: []*config.Node{
					&config.Node{
						Storage: "praefect-internal-1",
						Token:   "abc",
					},
					&config.Node{
						Storage: "praefect-internal-2",
						Token:   "abc",
					},
				},
			},
		},
	}

	t.Run("gitaly responds with ok", func(t *testing.T) {
		cc, _, cleanup := runPraefectServerWithGitaly(t, gconfig.Config, conf)
		defer cleanup()

		expected := &gitalypb.ServerInfoResponse{
			ServerVersion: version.GetVersion(),
			GitVersion:    gitVersion,
			StorageStatuses: []*gitalypb.ServerInfoResponse_StorageStatus{
				{StorageName: "virtual-storage-1", Readable: true, Writeable: true, ReplicationFactor: 2},
			},
		}

		client := gitalypb.NewServerServiceClient(cc)
		actual, err := client.ServerInfo(ctx, &gitalypb.ServerInfoRequest{})
		require.NoError(t, err)
		for _, ss := range actual.StorageStatuses {
			ss.FsType = ""
			ss.FilesystemId = ""
		}
		require.True(t, proto.Equal(expected, actual), "expected: %v, got: %v", expected, actual)
	})

	t.Run("gitaly responds with error", func(t *testing.T) {
		backends := map[string]mock.SimpleServiceServer{
			conf.VirtualStorages[0].Nodes[0].Storage: &mockSvc{},
			conf.VirtualStorages[0].Nodes[1].Storage: &mockSvc{},
		}

		cc, _, cleanup := runPraefectServerWithMock(t, conf, nil, backends)
		defer cleanup()

		client := gitalypb.NewServerServiceClient(cc)
		actual, err := client.ServerInfo(ctx, &gitalypb.ServerInfoRequest{})
		require.NoError(t, err, "we expect praefect's server info to fail open even if the gitaly calls result in an error")
		require.Empty(t, actual.StorageStatuses, "got: %v", actual)
	})
}

func TestGitalyServerInfoBadNode(t *testing.T) {
	gitalySocket := testhelper.GetTemporaryGitalySocketFileName(t)
	_, healthSrv := testhelper.NewServerWithHealth(t, gitalySocket)
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_UNKNOWN)

	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			&config.VirtualStorage{
				Nodes: []*config.Node{
					&config.Node{
						Storage: "praefect-internal-1",
						Address: "unix://" + gitalySocket,
						Token:   "abc",
					},
				},
			},
		},
	}

	cc, _, cleanup := runPraefectServer(t, conf, buildOptions{})
	defer cleanup()

	client := gitalypb.NewServerServiceClient(cc)

	ctx, cancel := testhelper.Context()
	defer cancel()

	metadata, err := client.ServerInfo(ctx, &gitalypb.ServerInfoRequest{})
	require.NoError(t, err)
	require.Len(t, metadata.GetStorageStatuses(), 0)
}

func TestGitalyDiskStatistics(t *testing.T) {
	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Nodes: []*config.Node{
					{
						Storage: "praefect-internal-1",
						Token:   "abc",
					},
					{
						Storage: "praefect-internal-2",
						Token:   "abc",
					}},
			},
		},
	}

	cc, _, cleanup := runPraefectServerWithGitaly(t, gconfig.Config, conf)
	defer cleanup()

	client := gitalypb.NewServerServiceClient(cc)

	ctx, cancel := testhelper.Context()
	defer cancel()

	metadata, err := client.DiskStatistics(ctx, &gitalypb.DiskStatisticsRequest{})
	require.NoError(t, err)
	// flattenVirtualStoragesToStoragePath flattens nodes to gitaly storages causing each of two gitaly instances to have 2 storages
	require.Len(t, metadata.GetStorageStatuses(), 4)

	for _, storageStatus := range metadata.GetStorageStatuses() {
		require.NotNil(t, storageStatus, "none of the storage statuses should be nil")
	}
}

func TestHealthCheck(t *testing.T) {
	cc, _, cleanup := runPraefectServerWithGitaly(t, gconfig.Config, testConfig(1))
	defer cleanup()

	ctx, cancel := testhelper.Context(testhelper.ContextWithTimeout(time.Second))
	defer cancel()

	client := grpc_health_v1.NewHealthClient(cc)
	_, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)
}

func TestRejectBadStorage(t *testing.T) {
	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			&config.VirtualStorage{
				Name: "praefect",
				Nodes: []*config.Node{
					&config.Node{
						Storage: "praefect-internal-0",
						Address: "tcp::/this-doesnt-matter",
					},
				},
			},
		},
	}

	cc, _, cleanup := runPraefectServerWithGitaly(t, gconfig.Config, conf)
	defer cleanup()

	badTargetRepo := gitalypb.Repository{
		StorageName:  "default",
		RelativePath: "/path/to/hashed/storage",
	}

	repoClient := gitalypb.NewRepositoryServiceClient(cc)

	ctx, cancel := testhelper.Context()
	defer cancel()

	_, err := repoClient.GarbageCollect(ctx, &gitalypb.GarbageCollectRequest{Repository: &badTargetRepo})
	require.Error(t, err)
}

func TestWarnDuplicateAddrs(t *testing.T) {
	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			&config.VirtualStorage{
				Name: "default",
				Nodes: []*config.Node{
					&config.Node{
						Storage: "praefect-internal-0",
						Address: "tcp://abc",
					},
					&config.Node{
						Storage: "praefect-internal-1",
						Address: "tcp://xyz",
					},
				},
			},
			&config.VirtualStorage{
				Name: "praefect",
				Nodes: []*config.Node{
					&config.Node{
						Storage: "praefect-internal-0",
						Address: "tcp://abc",
					},
					&config.Node{
						Storage: "praefect-internal-1",
						Address: "tcp://xyz",
					},
				},
			},
		},
	}

	tLogger, hook := test.NewNullLogger()

	// instantiate a praefect server and trigger warning
	_, _, cleanup := runPraefectServer(t, conf, buildOptions{
		withLogger:  logrus.NewEntry(tLogger),
		withNodeMgr: nullNodeMgr{}, // to suppress node address issues
	})
	defer cleanup()

	for _, entry := range hook.AllEntries() {
		require.NotContains(t, entry.Message, "more than one backend node")
	}

	conf = config.Config{
		VirtualStorages: []*config.VirtualStorage{
			&config.VirtualStorage{
				Name: "praefect",
				Nodes: []*config.Node{
					&config.Node{
						Storage: "praefect-internal-0",
						Address: "tcp::/samesies",
					},
					&config.Node{
						Storage: "praefect-internal-1",
						Address: "tcp::/samesies",
					},
				},
			},
		},
	}

	tLogger, hook = test.NewNullLogger()

	// instantiate a praefect server and trigger warning
	_, _, cleanup = runPraefectServer(t, conf, buildOptions{
		withLogger:  logrus.NewEntry(tLogger),
		withNodeMgr: nullNodeMgr{}, // to suppress node address issues
	})
	defer cleanup()

	var found bool
	for _, entry := range hook.AllEntries() {
		if strings.Contains(entry.Message, "more than one backend node") {
			found = true
			break
		}
	}
	require.True(t, found, "expected to find error log")

	conf = config.Config{
		VirtualStorages: []*config.VirtualStorage{
			&config.VirtualStorage{
				Name: "default",
				Nodes: []*config.Node{
					&config.Node{
						Storage: "praefect-internal-0",
						Address: "tcp://abc",
					},
					&config.Node{
						Storage: "praefect-internal-1",
						Address: "tcp://xyz",
					},
				},
			},
			&config.VirtualStorage{
				Name: "praefect",
				Nodes: []*config.Node{
					&config.Node{
						Storage: "praefect-internal-0",
						Address: "tcp://abc",
					},
					&config.Node{
						Storage: "praefect-internal-2",
						Address: "tcp://xyz",
					},
				},
			},
		},
	}

	tLogger, hook = test.NewNullLogger()

	// instantiate a praefect server and trigger warning
	_, _, cleanup = runPraefectServer(t, conf, buildOptions{
		withLogger:  logrus.NewEntry(tLogger),
		withNodeMgr: nullNodeMgr{}, // to suppress node address issues
	})
	defer cleanup()

	for _, entry := range hook.AllEntries() {
		require.NotContains(t, entry.Message, "more than one backend node")
	}
}

func TestRepoRemoval(t *testing.T) {
	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			&config.VirtualStorage{
				Name: "praefect",
				Nodes: []*config.Node{
					&config.Node{
						Storage: gconfig.Config.Storages[0].Name,
						Address: "tcp::/samesies",
					},
					&config.Node{
						Storage: "praefect-internal-1",
						Address: "tcp::/this-doesnt-matter",
					},
					&config.Node{
						Storage: "praefect-internal-2",
						Address: "tcp::/this-doesnt-matter",
					},
				},
			},
		},
	}

	defer func(storages []gconfig.Storage) {
		gconfig.Config.Storages = storages
	}(gconfig.Config.Storages)

	testStorages := []gconfig.Storage{
		{
			Name: conf.VirtualStorages[0].Nodes[1].Storage,
			Path: tempStoragePath(t),
		},
		{
			Name: conf.VirtualStorages[0].Nodes[2].Storage,
			Path: tempStoragePath(t),
		},
	}
	gconfig.Config.Storages = append(gconfig.Config.Storages, testStorages...)
	defer func() {
		for _, s := range testStorages {
			require.NoError(t, os.RemoveAll(s.Path))
		}
	}()

	locator := gconfig.NewLocator(gconfig.Config)

	tRepo, _, tCleanup := testhelper.NewTestRepo(t)
	defer tCleanup()

	_, path1, cleanup1 := cloneRepoAtStorage(t, locator, tRepo, conf.VirtualStorages[0].Nodes[1].Storage)
	defer cleanup1()
	_, path2, cleanup2 := cloneRepoAtStorage(t, locator, tRepo, conf.VirtualStorages[0].Nodes[2].Storage)
	defer cleanup2()

	// prerequisite: repos should exist at expected paths
	require.DirExists(t, path1)
	require.DirExists(t, path2)

	// TODO: once https://gitlab.com/gitlab-org/gitaly/-/issues/2703 is done and the replication manager supports
	// graceful shutdown, we can remove this code that waits for jobs to be complete
	queueInterceptor := datastore.NewReplicationEventQueueInterceptor(datastore.NewMemoryReplicationEventQueue(conf))

	jobsDoneCh := make(chan struct{}, 2)
	queueInterceptor.OnAcknowledge(func(ctx context.Context, state datastore.JobState, ids []uint64, queue datastore.ReplicationEventQueue) ([]uint64, error) {
		if state == datastore.JobStateCompleted {
			jobsDoneCh <- struct{}{}
		}

		return queue.Acknowledge(ctx, state, ids)
	})

	cc, _, cleanup := runPraefectServerWithGitalyWithDatastore(t, gconfig.Config, conf, queueInterceptor)
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	virtualRepo := *tRepo
	virtualRepo.StorageName = conf.VirtualStorages[0].Name

	rClient := gitalypb.NewRepositoryServiceClient(cc)

	_, err := rClient.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{
		Repository: &virtualRepo,
	})
	require.NoError(t, err)

	storage, ok := gconfig.Config.Storage(conf.VirtualStorages[0].Nodes[0].Storage)
	require.True(t, ok)
	testhelper.AssertPathNotExists(t, filepath.Join(storage.Path, tRepo.RelativePath))

	var jobsDone int
	for {
		<-jobsDoneCh
		jobsDone++
		if jobsDone == 2 {
			break
		}
	}

	testhelper.AssertPathNotExists(t, path1)
	testhelper.AssertPathNotExists(t, path2)
}

func pollUntilRemoved(t testing.TB, path string, deadline <-chan time.Time) {
	for {
		select {
		case <-deadline:
			require.Failf(t, "unable to detect path removal for %s", path)
		default:
			_, err := os.Stat(path)
			if os.IsNotExist(err) {
				return
			}
			require.NoError(t, err, "unexpected error while checking path %s", path)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRepoRename(t *testing.T) {
	oldStorages := gconfig.Config.Storages
	defer func() { gconfig.Config.Storages = oldStorages }()

	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "praefect",
				Nodes: []*config.Node{
					0: {
						Storage: gconfig.Config.Storages[0].Name,
						Address: "tcp::/this-doesnt-matter",
					},
					1: {
						Storage: "gitaly-1",
						Address: "tcp::/this-doesnt-matter",
					},
					2: {
						Storage: "gitaly-2",
						Address: "tcp::/this-doesnt-matter",
					},
				},
			},
		},
		Failover: config.Failover{Enabled: true},
	}

	virtualStorage := conf.VirtualStorages[0]
	testStorages := []gconfig.Storage{
		{
			Name: virtualStorage.Nodes[1].Storage,
			Path: tempStoragePath(t),
		},
		{
			Name: virtualStorage.Nodes[2].Storage,
			Path: tempStoragePath(t),
		},
	}

	gconfig.Config.Storages = append(gconfig.Config.Storages, testStorages...)
	defer func() {
		for _, s := range testStorages {
			require.NoError(t, os.RemoveAll(s.Path))
		}
	}()

	require.Len(t, gconfig.Config.Storages, 3, "1 default storage and 2 replicas of it")

	locator := gconfig.NewLocator(gconfig.Config)

	// repo0 is a template that is used to create replica set by cloning it into other storage (directories)
	repo0, path0, cleanup0 := testhelper.NewTestRepo(t)
	defer cleanup0()

	_, path1, cleanup1 := cloneRepoAtStorage(t, locator, repo0, virtualStorage.Nodes[1].Storage)
	defer cleanup1()

	_, path2, cleanup2 := cloneRepoAtStorage(t, locator, repo0, virtualStorage.Nodes[2].Storage)
	defer cleanup2()

	var canCheckRepo sync.WaitGroup
	canCheckRepo.Add(2)

	evq := datastore.NewReplicationEventQueueInterceptor(datastore.NewMemoryReplicationEventQueue(conf))
	evq.OnAcknowledge(func(ctx context.Context, state datastore.JobState, ids []uint64, queue datastore.ReplicationEventQueue) ([]uint64, error) {
		defer canCheckRepo.Done()
		return queue.Acknowledge(ctx, state, ids)
	})

	cc, _, cleanup := runPraefectServerWithGitalyWithDatastore(t, gconfig.Config, conf, evq)
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	// virtualRepo is a virtual repository all requests to it would be applied to the underline Gitaly nodes behind it
	cpRepo0 := *repo0
	virtualRepo := &cpRepo0
	virtualRepo.StorageName = virtualStorage.Name

	repoServiceClient := gitalypb.NewRepositoryServiceClient(cc)

	newName, err := text.RandomHex(20)
	require.NoError(t, err)

	expNewPath0 := filepath.Join(gconfig.Config.Storages[0].Path, newName)
	expNewPath1 := filepath.Join(gconfig.Config.Storages[1].Path, newName)
	expNewPath2 := filepath.Join(gconfig.Config.Storages[2].Path, newName)

	require.NoError(t, os.RemoveAll(expNewPath0), "target dir must not exist before renaming")
	require.NoError(t, os.RemoveAll(expNewPath1), "target dir must not exist before renaming")
	require.NoError(t, os.RemoveAll(expNewPath2), "target dir must not exist before renaming")

	_, err = repoServiceClient.RenameRepository(ctx, &gitalypb.RenameRepositoryRequest{
		Repository:   virtualRepo,
		RelativePath: newName,
	})
	require.NoError(t, err)

	resp, err := repoServiceClient.RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{
		Repository: virtualRepo,
	})
	require.NoError(t, err)
	require.False(t, resp.GetExists(), "repo with old name must gone")

	// as we renamed the repo we need to update RelativePath before we could check if it exists
	cpVirtualRepo := *virtualRepo
	renamedVirtualRepo := &cpVirtualRepo
	renamedVirtualRepo.RelativePath = newName

	// wait until replication jobs propagate changes to other storages
	// as we don't know which one will be used to check because of read distribution
	canCheckRepo.Wait()

	resp, err = repoServiceClient.RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{
		Repository: renamedVirtualRepo,
	})
	require.NoError(t, err)
	require.True(t, resp.GetExists(), "repo with new name must exist")
	require.DirExists(t, expNewPath0, "must be renamed on secondary from %q to %q", path0, expNewPath0)
	defer func() { require.NoError(t, os.RemoveAll(expNewPath0)) }()

	// the renaming of the repo on the secondary servers is not deterministic
	// since it relies on eventually consistent replication
	pollUntilRemoved(t, path1, time.After(10*time.Second))
	require.DirExists(t, expNewPath1, "must be renamed on secondary from %q to %q", path1, expNewPath1)
	defer func() { require.NoError(t, os.RemoveAll(expNewPath1)) }()

	pollUntilRemoved(t, path2, time.After(10*time.Second))
	require.DirExists(t, expNewPath2, "must be renamed on secondary from %q to %q", path2, expNewPath2)
	defer func() { require.NoError(t, os.RemoveAll(expNewPath2)) }()
}

type mockSmartHTTP struct {
	txMgr         *transactions.Manager
	m             sync.Mutex
	methodsCalled map[string]int
}

func (m *mockSmartHTTP) InfoRefsUploadPack(req *gitalypb.InfoRefsRequest, stream gitalypb.SmartHTTPService_InfoRefsUploadPackServer) error {
	m.m.Lock()
	defer m.m.Unlock()
	if m.methodsCalled == nil {
		m.methodsCalled = make(map[string]int)
	}

	m.methodsCalled["InfoRefsUploadPack"]++

	stream.Send(&gitalypb.InfoRefsResponse{})
	return nil
}

func (m *mockSmartHTTP) InfoRefsReceivePack(req *gitalypb.InfoRefsRequest, stream gitalypb.SmartHTTPService_InfoRefsReceivePackServer) error {
	m.m.Lock()
	defer m.m.Unlock()
	if m.methodsCalled == nil {
		m.methodsCalled = make(map[string]int)
	}

	m.methodsCalled["InfoRefsReceivePack"]++

	stream.Send(&gitalypb.InfoRefsResponse{})
	return nil
}

func (m *mockSmartHTTP) PostUploadPack(stream gitalypb.SmartHTTPService_PostUploadPackServer) error {
	m.m.Lock()
	defer m.m.Unlock()
	if m.methodsCalled == nil {
		m.methodsCalled = make(map[string]int)
	}

	m.methodsCalled["PostUploadPack"]++

	stream.Send(&gitalypb.PostUploadPackResponse{})
	return nil
}

func (m *mockSmartHTTP) PostReceivePack(stream gitalypb.SmartHTTPService_PostReceivePackServer) error {
	m.m.Lock()
	defer m.m.Unlock()
	if m.methodsCalled == nil {
		m.methodsCalled = make(map[string]int)
	}

	m.methodsCalled["PostReceivePack"]++

	var err error
	var req *gitalypb.PostReceivePackRequest
	for {
		req, err = stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return helper.ErrInternal(err)
		}

		if err := stream.Send(&gitalypb.PostReceivePackResponse{Data: req.GetData()}); err != nil {
			return helper.ErrInternal(err)
		}
	}

	ctx := stream.Context()

	tx, err := metadata.TransactionFromContext(ctx)
	if err != nil {
		return helper.ErrInternal(err)
	}

	hash := sha1.Sum([]byte{})
	if err := m.txMgr.VoteTransaction(ctx, tx.ID, tx.Node, hash[:]); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

func (m *mockSmartHTTP) Called(method string) int {
	m.m.Lock()
	defer m.m.Unlock()

	return m.methodsCalled[method]
}

func newSmartHTTPGrpcServer(t *testing.T, srv gitalypb.SmartHTTPServiceServer) (string, *grpc.Server) {
	socketPath := testhelper.GetTemporaryGitalySocketFileName(t)
	listener, err := net.Listen("unix", socketPath)
	require.NoError(t, err)

	grpcServer := testhelper.NewTestGrpcServer(t, nil, nil)

	healthSrvr := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthSrvr)
	healthSrvr.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	gitalypb.RegisterSmartHTTPServiceServer(grpcServer, srv)

	go grpcServer.Serve(listener)

	return socketPath, grpcServer
}

func TestProxyWrites(t *testing.T) {
	txMgr := transactions.NewManager(config.Config{})

	smartHTTP0, smartHTTP1, smartHTTP2 := &mockSmartHTTP{txMgr: txMgr}, &mockSmartHTTP{txMgr: txMgr}, &mockSmartHTTP{txMgr: txMgr}

	socket0, srv0 := newSmartHTTPGrpcServer(t, smartHTTP0)
	defer srv0.Stop()
	socket1, srv1 := newSmartHTTPGrpcServer(t, smartHTTP1)
	defer srv1.Stop()
	socket2, srv2 := newSmartHTTPGrpcServer(t, smartHTTP2)
	defer srv2.Stop()

	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "default",
				Nodes: []*config.Node{
					{
						Storage: "praefect-internal-0",
						Address: "unix://" + socket0,
					},
					{
						Storage: "praefect-internal-1",
						Address: "unix://" + socket1,
					},
					{
						Storage: "praefect-internal-2",
						Address: "unix://" + socket2,
					},
				},
			},
		},
	}

	queue := datastore.NewMemoryReplicationEventQueue(conf)
	entry := testhelper.DiscardTestEntry(t)

	nodeMgr, err := nodes.NewManager(entry, conf, nil, nil, promtest.NewMockHistogramVec(), protoregistry.GitalyProtoPreregistered, nil)
	require.NoError(t, err)
	nodeMgr.Start(0, time.Hour)

	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	rs := datastore.MockRepositoryStore{
		GetConsistentStoragesFunc: func(ctx context.Context, virtualStorage, relativePath string) (map[string]struct{}, error) {
			return map[string]struct{}{"praefect-internal-0": {}, "praefect-internal-1": {}, "praefect-internal-2": {}}, nil
		},
	}

	coordinator := NewCoordinator(
		queue,
		rs,
		NewNodeManagerRouter(nodeMgr, rs),
		txMgr,
		conf,
		protoregistry.GitalyProtoPreregistered,
	)

	server := grpc.NewServer(
		grpc.CustomCodec(proxy.NewCodec()),
		grpc.UnknownServiceHandler(proxy.TransparentHandler(coordinator.StreamDirector)),
	)

	socket := testhelper.GetTemporaryGitalySocketFileName(t)
	listener, err := net.Listen("unix", socket)
	require.NoError(t, err)

	go server.Serve(listener)
	defer server.Stop()

	client, _ := newSmartHTTPClient(t, "unix://"+socket)

	shard, err := nodeMgr.GetShard(ctx, conf.VirtualStorages[0].Name)
	require.NoError(t, err)

	for _, storage := range conf.VirtualStorages[0].Nodes {
		node, err := shard.GetNode(storage.Storage)
		require.NoError(t, err)
		waitNodeToChangeHealthStatus(ctx, t, node, true)
	}

	stream, err := client.PostReceivePack(ctx)
	require.NoError(t, err)

	payload := "some pack data"
	for i := 0; i < 10; i++ {
		require.NoError(t, stream.Send(&gitalypb.PostReceivePackRequest{
			Repository: testRepo,
			Data:       []byte(payload),
		}))
	}

	require.NoError(t, stream.CloseSend())

	var receivedData bytes.Buffer
	for {
		resp, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			require.FailNowf(t, "unexpected non io.EOF error: %v", err.Error())
		}

		_, err = receivedData.Write(resp.GetData())
		require.NoError(t, err)
	}

	assert.Equal(t, 1, smartHTTP0.Called("PostReceivePack"))
	assert.Equal(t, 1, smartHTTP1.Called("PostReceivePack"))
	assert.Equal(t, 1, smartHTTP2.Called("PostReceivePack"))
	assert.Equal(t, bytes.Repeat([]byte(payload), 10), receivedData.Bytes())
}

func TestErrorThreshold(t *testing.T) {
	backendToken := ""
	backend, cleanup := newMockDownstream(t, backendToken, &mockSvc{
		repoMutatorUnary: func(ctx context.Context, req *mock.RepoRequest) (*empty.Empty, error) {
			md, ok := grpc_metadata.FromIncomingContext(ctx)
			if !ok {
				return &empty.Empty{}, errors.New("couldn't read metadata")
			}

			if md.Get("bad-header")[0] == "true" {
				return &empty.Empty{}, helper.ErrInternalf("something went wrong")
			}

			return &empty.Empty{}, nil
		},
		repoAccessorUnary: func(ctx context.Context, req *mock.RepoRequest) (*empty.Empty, error) {
			md, ok := grpc_metadata.FromIncomingContext(ctx)
			if !ok {
				return &empty.Empty{}, errors.New("couldn't read metadata")
			}

			if md.Get("bad-header")[0] == "true" {
				return &empty.Empty{}, helper.ErrInternalf("something went wrong")
			}

			return &empty.Empty{}, nil
		},
	})
	defer cleanup()

	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "default",
				Nodes: []*config.Node{
					{
						Storage: "praefect-internal-0",
						Address: backend,
					},
				},
			},
		},
		Failover: config.Failover{
			Enabled:          true,
			ElectionStrategy: "local",
		},
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	queue := datastore.NewMemoryReplicationEventQueue(conf)
	entry := testhelper.DiscardTestEntry(t)

	testCases := []struct {
		desc     string
		accessor bool
		error    error
	}{
		{
			desc:     "read threshold reached",
			accessor: true,
			error:    errors.New(`read error threshold reached for storage "praefect-internal-0"`),
		},
		{
			desc:  "write threshold reached",
			error: errors.New(`write error threshold reached for storage "praefect-internal-0"`),
		},
	}

	gz := proto.FileDescriptor("praefect/mock/mock.proto")
	fd, err := protoregistry.ExtractFileDescriptor(gz)
	require.NoError(t, err)

	registry, err := protoregistry.New(fd)
	require.NoError(t, err)

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			readThreshold := uint32(5000)
			writeThreshold := uint32(5)
			if tc.accessor {
				readThreshold = 5
				writeThreshold = 5000
			}

			errorTracker, err := tracker.NewErrors(ctx, 10*time.Hour, readThreshold, writeThreshold)
			require.NoError(t, err)

			rs := datastore.MockRepositoryStore{}
			sp := datastore.NewDirectStorageProvider(rs)
			nodeMgr, err := nodes.NewManager(entry, conf, nil, sp, promtest.NewMockHistogramVec(), registry, errorTracker)
			require.NoError(t, err)

			coordinator := NewCoordinator(
				queue,
				rs,
				NewNodeManagerRouter(nodeMgr, rs),
				nil,
				conf,
				registry,
			)

			server := grpc.NewServer(
				grpc.CustomCodec(proxy.NewCodec()),
				grpc.UnknownServiceHandler(proxy.TransparentHandler(coordinator.StreamDirector)),
			)

			socket := testhelper.GetTemporaryGitalySocketFileName(t)
			listener, err := net.Listen("unix", socket)
			require.NoError(t, err)

			go server.Serve(listener)
			defer server.Stop()

			conn, err := dial("unix://"+socket, []grpc.DialOption{grpc.WithInsecure()})
			require.NoError(t, err)
			cli := mock.NewSimpleServiceClient(conn)

			repo, _, cleanup := testhelper.NewTestRepo(t)
			defer cleanup()

			node := nodeMgr.Nodes()["default"][0]
			require.Equal(t, "praefect-internal-0", node.GetStorage())

			// to get the node in a healthy starting state, three consecutive successful checks
			// are needed
			for i := 0; i < 3; i++ {
				healthy, err := node.CheckHealth(ctx)
				require.NoError(t, err)
				require.True(t, healthy)
			}

			for i := 0; i < 5; i++ {
				ctx := grpc_metadata.AppendToOutgoingContext(ctx, "bad-header", "true")

				handler := cli.RepoMutatorUnary
				if tc.accessor {
					handler = cli.RepoAccessorUnary
				}

				healthy, err := node.CheckHealth(ctx)
				require.NoError(t, err)
				require.True(t, healthy)

				_, err = handler(ctx, &mock.RepoRequest{Repo: repo})
				require.Equal(t, status.Error(codes.Internal, "something went wrong"), err)
			}

			healthy, err := node.CheckHealth(ctx)
			require.Equal(t, tc.error, err)
			require.False(t, healthy)
		})
	}
}

func newSmartHTTPClient(t *testing.T, serverSocketPath string) (gitalypb.SmartHTTPServiceClient, *grpc.ClientConn) {
	t.Helper()

	conn, err := grpc.Dial(serverSocketPath, grpc.WithInsecure())
	require.NoError(t, err)

	return gitalypb.NewSmartHTTPServiceClient(conn), conn
}

func tempStoragePath(t testing.TB) string {
	p, err := ioutil.TempDir("", t.Name())
	require.NoError(t, err)
	return p
}

func cloneRepoAtStorage(t testing.TB, locator storage.Locator, src *gitalypb.Repository, storageName string) (*gitalypb.Repository, string, func()) {
	dst := *src
	dst.StorageName = storageName

	dstP, err := locator.GetPath(&dst)
	require.NoError(t, err)

	srcP, err := locator.GetPath(src)
	require.NoError(t, err)

	require.NoError(t, os.MkdirAll(dstP, 0755))
	testhelper.MustRunCommand(t, nil, "git",
		"clone", "--no-hardlinks", "--dissociate", "--bare", srcP, dstP)

	return &dst, dstP, func() { require.NoError(t, os.RemoveAll(dstP)) }
}
