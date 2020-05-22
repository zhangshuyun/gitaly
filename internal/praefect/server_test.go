package praefect

import (
	"context"
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
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gconfig "gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/fieldextractors"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/sentryhandler"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/proxy"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/mock"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/transactions"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/promtest"
	"gitlab.com/gitlab-org/gitaly/internal/version"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

func TestServerRouteServerAccessor(t *testing.T) {
	var (
		conf = testConfig(1)
		reqQ = make(chan *mock.SimpleRequest)

		expectResp = &mock.SimpleResponse{Value: 2}

		// note: a server scoped RPC will be randomly routed
		// to an available backend server. To simplify our
		// test, a single backend server is used.
		backends = map[string]mock.SimpleServiceServer{
			conf.VirtualStorages[0].Nodes[0].Storage: &mockSvc{
				serverAccessor: func(_ context.Context, req *mock.SimpleRequest) (*mock.SimpleResponse, error) {
					reqQ <- req
					return expectResp, nil
				},
			},
		}
	)

	cc, _, cleanup := runPraefectServerWithMock(t, conf, nil, backends)
	defer cleanup()

	cli := mock.NewSimpleServiceClient(cc)

	expectReq := &mock.SimpleRequest{Value: 1}

	done := make(chan struct{})
	go func() {
		defer close(done)

		actualReq := <-reqQ
		assert.True(t, proto.Equal(expectReq, actualReq),
			"received unexpected request value: %+v instead of %+v", actualReq, expectReq)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	actualResp, err := cli.ServerAccessor(ctx, expectReq)
	require.NoError(t, err)
	require.True(t, proto.Equal(expectResp, actualResp),
		"expected response was not routed back")

	waitUntil(t, done, time.Second)
}

func TestGitalyServerInfo(t *testing.T) {
	gitVersion, err := git.Version()
	require.NoError(t, err)

	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			&config.VirtualStorage{
				Name: "virtual-storage-1",
				Nodes: []*config.Node{
					&config.Node{
						Storage:        "praefect-internal-1",
						DefaultPrimary: true,
						Token:          "abc",
					},
					&config.Node{
						Storage: "praefect-internal-2",
						Token:   "abc",
					},
				},
			},
		},
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	t.Run("gitaly responds with ok", func(t *testing.T) {
		cc, _, cleanup := runPraefectServerWithGitaly(t, conf)
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
	gitalySocket := testhelper.GetTemporaryGitalySocketFileName()
	_, healthSrv := testhelper.NewServerWithHealth(t, gitalySocket)
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_UNKNOWN)

	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			&config.VirtualStorage{
				Nodes: []*config.Node{
					&config.Node{
						Storage:        "praefect-internal-1",
						Address:        "unix://" + gitalySocket,
						DefaultPrimary: true,
						Token:          "abc",
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
						Storage:        "praefect-internal-1",
						DefaultPrimary: true,
						Token:          "abc",
					},
					{
						Storage: "praefect-internal-2",
						Token:   "abc",
					}},
			},
		},
	}

	cc, _, cleanup := runPraefectServerWithGitaly(t, conf)
	defer cleanup()

	client := gitalypb.NewServerServiceClient(cc)

	ctx, cancel := testhelper.Context()
	defer cancel()

	metadata, err := client.DiskStatistics(ctx, &gitalypb.DiskStatisticsRequest{})
	require.NoError(t, err)
	require.Len(t, metadata.GetStorageStatuses(), len(conf.VirtualStorages[0].Nodes))

	for _, storageStatus := range metadata.GetStorageStatuses() {
		require.NotNil(t, storageStatus, "none of the storage statuses should be nil")
	}
}

func TestHealthCheck(t *testing.T) {
	cc, _, cleanup := runPraefectServerWithGitaly(t, testConfig(1))
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
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
						DefaultPrimary: true,
						Storage:        "praefect-internal-0",
						Address:        "tcp::/this-doesnt-matter",
					},
				},
			},
		},
	}

	cc, _, cleanup := runPraefectServerWithGitaly(t, conf)
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
						DefaultPrimary: true,
						Storage:        "praefect-internal-0",
						Address:        "tcp://abc",
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
						DefaultPrimary: true,
						Storage:        "praefect-internal-0",
						Address:        "tcp://abc",
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

	for _, entry := range hook.Entries {
		require.NotContains(t, entry.Message, "more than one backend node")
	}

	conf = config.Config{
		VirtualStorages: []*config.VirtualStorage{
			&config.VirtualStorage{
				Name: "praefect",
				Nodes: []*config.Node{
					&config.Node{
						DefaultPrimary: true,
						Storage:        "praefect-internal-0",
						Address:        "tcp::/samesies",
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
	for _, entry := range hook.Entries {
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
						DefaultPrimary: true,
						Storage:        "praefect-internal-0",
						Address:        "tcp://abc",
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
						DefaultPrimary: true,
						Storage:        "praefect-internal-0",
						Address:        "tcp://abc",
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

	for _, entry := range hook.Entries {
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
						DefaultPrimary: true,
						Storage:        gconfig.Config.Storages[0].Name,
						Address:        "tcp::/samesies",
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

	tRepo, _, tCleanup := testhelper.NewTestRepo(t)
	defer tCleanup()

	_, path1, cleanup1 := cloneRepoAtStorage(t, tRepo, conf.VirtualStorages[0].Nodes[1].Storage)
	defer cleanup1()
	_, path2, cleanup2 := cloneRepoAtStorage(t, tRepo, conf.VirtualStorages[0].Nodes[2].Storage)
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

	cc, _, cleanup := runPraefectServerWithGitalyWithDatastore(t, conf, queueInterceptor)
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

	resp, err := rClient.RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{
		Repository: &virtualRepo,
	})
	require.NoError(t, err)
	require.Equal(t, false, resp.GetExists())

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
						DefaultPrimary: true,
						Storage:        gconfig.Config.Storages[0].Name,
						Address:        "tcp::/this-doesnt-matter",
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

	// repo0 is a template that is used to create replica set by cloning it into other storage (directories)
	repo0, path0, cleanup0 := testhelper.NewTestRepo(t)
	defer cleanup0()

	_, path1, cleanup1 := cloneRepoAtStorage(t, repo0, virtualStorage.Nodes[1].Storage)
	defer cleanup1()

	_, path2, cleanup2 := cloneRepoAtStorage(t, repo0, virtualStorage.Nodes[2].Storage)
	defer cleanup2()

	var canCheckRepo sync.WaitGroup
	canCheckRepo.Add(2)

	evq := datastore.NewReplicationEventQueueInterceptor(datastore.NewMemoryReplicationEventQueue(conf))
	evq.OnAcknowledge(func(ctx context.Context, state datastore.JobState, ids []uint64, queue datastore.ReplicationEventQueue) ([]uint64, error) {
		defer canCheckRepo.Done()
		return queue.Acknowledge(ctx, state, ids)
	})

	cc, _, cleanup := runPraefectServerWithGitalyWithDatastore(t, conf, evq)
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

type mockSmartHttp struct {
	m             sync.Mutex
	methodsCalled map[string]int
}

func (m *mockSmartHttp) InfoRefsUploadPack(req *gitalypb.InfoRefsRequest, stream gitalypb.SmartHTTPService_InfoRefsUploadPackServer) error {
	m.m.Lock()
	defer m.m.Unlock()
	if m.methodsCalled == nil {
		m.methodsCalled = make(map[string]int)
	}

	m.methodsCalled["InfoRefsUploadPack"] += 1

	stream.Send(&gitalypb.InfoRefsResponse{})
	return nil
}

func (m *mockSmartHttp) InfoRefsReceivePack(req *gitalypb.InfoRefsRequest, stream gitalypb.SmartHTTPService_InfoRefsReceivePackServer) error {
	m.m.Lock()
	defer m.m.Unlock()
	if m.methodsCalled == nil {
		m.methodsCalled = make(map[string]int)
	}

	m.methodsCalled["InfoRefsReceivePack"] += 1

	stream.Send(&gitalypb.InfoRefsResponse{})
	return nil
}

func (m *mockSmartHttp) PostUploadPack(stream gitalypb.SmartHTTPService_PostUploadPackServer) error {
	m.m.Lock()
	defer m.m.Unlock()
	if m.methodsCalled == nil {
		m.methodsCalled = make(map[string]int)
	}

	m.methodsCalled["PostUploadPack"] += 1

	stream.Send(&gitalypb.PostUploadPackResponse{})
	return nil
}

func (m *mockSmartHttp) PostReceivePack(stream gitalypb.SmartHTTPService_PostReceivePackServer) error {
	m.m.Lock()
	defer m.m.Unlock()
	if m.methodsCalled == nil {
		m.methodsCalled = make(map[string]int)
	}

	m.methodsCalled["PostReceivePack"] += 1

	stream.Send(&gitalypb.PostReceivePackResponse{})
	return nil
}

func (m *mockSmartHttp) Called(method string) int {
	m.m.Lock()
	defer m.m.Unlock()

	return m.methodsCalled[method]
}

func newGrpcServer(t *testing.T, srv gitalypb.SmartHTTPServiceServer) (string, *grpc.Server) {
	grpcSrv := testhelper.NewTestGrpcServer(t, nil, nil)
	socketPath := testhelper.GetTemporaryGitalySocketFileName()

	gitalypb.RegisterSmartHTTPServiceServer(grpcSrv, srv)
	reflection.Register(grpcSrv)

	listener, err := net.Listen("unix", socketPath)
	require.NoError(t, err)

	go func() { grpcSrv.Serve(listener) }()

	return socketPath, grpcSrv
}

func TestProxyWrites(t *testing.T) {
	smartHttp0, smartHttp1, smartHttp2 := &mockSmartHttp{}, &mockSmartHttp{}, &mockSmartHttp{}

	socket0, srv0 := newGrpcServer(t, smartHttp0)
	defer srv0.Stop()
	socket1, srv1 := newGrpcServer(t, smartHttp1)
	defer srv1.Stop()
	socket2, srv2 := newGrpcServer(t, smartHttp2)
	defer srv2.Stop()

	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "default",
				Nodes: []*config.Node{
					{
						DefaultPrimary: true,
						Storage:        "praefect-internal-0",
						Address:        "unix://" + socket0,
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

	nodeMgr, err := nodes.NewManager(entry, conf, nil, queue, promtest.NewMockHistogramVec())
	require.NoError(t, err)
	txMgr := transactions.NewManager()

	coordinator := NewCoordinator(queue, nodeMgr, txMgr, conf, protoregistry.GitalyProtoPreregistered)

	server := grpc.NewServer(
		grpc.CustomCodec(proxy.NewCodec()),
		grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(
				// context tags usage is required by sentryhandler.StreamLogHandler
				grpc_ctxtags.StreamServerInterceptor(grpc_ctxtags.WithFieldExtractorForInitialReq(fieldextractors.FieldExtractor)),
				// sentry middleware to capture errors
				sentryhandler.StreamLogHandler,
			),
		),
		grpc.UnknownServiceHandler(proxy.TransparentHandler(coordinator.StreamDirector)),
	)

	socket := testhelper.GetTemporaryGitalySocketFileName()
	listener, err := net.Listen("unix", socket)
	require.NoError(t, err)

	go server.Serve(listener)
	defer server.Stop()

	client, _ := newSmartHTTPClient(t, "unix://"+socket)

	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx = featureflag.OutgoingCtxWithFeatureFlag(ctx, featureflag.ReferenceTransactions)

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	stream, err := client.PostReceivePack(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&gitalypb.PostReceivePackRequest{
		Repository: testRepo,
	}))
	require.NoError(t, stream.CloseSend())

	for {
		_, err = stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			require.FailNowf(t, "unexpected non io.EOF error: %v", err.Error())
		}
	}

	assert.Equal(t, 1, smartHttp0.Called("PostReceivePack"))
	assert.Equal(t, 1, smartHttp1.Called("PostReceivePack"))
	assert.Equal(t, 1, smartHttp2.Called("PostReceivePack"))
}

func newSmartHTTPClient(t *testing.T, serverSocketPath string) (gitalypb.SmartHTTPServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewSmartHTTPServiceClient(conn), conn
}

func tempStoragePath(t testing.TB) string {
	p, err := ioutil.TempDir("", t.Name())
	require.NoError(t, err)
	return p
}

func cloneRepoAtStorage(t testing.TB, src *gitalypb.Repository, storageName string) (*gitalypb.Repository, string, func()) {
	dst := *src
	dst.StorageName = storageName

	dstP, err := helper.GetPath(&dst)
	require.NoError(t, err)

	srcP, err := helper.GetPath(src)
	require.NoError(t, err)

	require.NoError(t, os.MkdirAll(dstP, 0755))
	testhelper.MustRunCommand(t, nil, "git",
		"clone", "--no-hardlinks", "--dissociate", "--bare", srcP, dstP)

	return &dst, dstP, func() { require.NoError(t, os.RemoveAll(dstP)) }
}
