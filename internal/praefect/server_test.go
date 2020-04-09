package praefect

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gconfig "gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/mock"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/promtest"
	"gitlab.com/gitlab-org/gitaly/internal/version"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
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

	cli, _, cleanup := runPraefectServerWithMock(t, conf, backends)
	defer cleanup()

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
	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			&config.VirtualStorage{
				Nodes: []*models.Node{
					&models.Node{
						Storage:        "praefect-internal-1",
						DefaultPrimary: true,
						Token:          "abc",
					},
					&models.Node{
						Storage: "praefect-internal-2",
						Token:   "abc",
					},
				},
			},
		},
	}

	cc, _, cleanup := runPraefectServerWithGitaly(t, conf)
	defer cleanup()

	client := gitalypb.NewServerServiceClient(cc)

	ctx, cancel := testhelper.Context()
	defer cancel()

	metadata, err := client.ServerInfo(ctx, &gitalypb.ServerInfoRequest{})
	require.NoError(t, err)
	require.Len(t, metadata.GetStorageStatuses(), len(conf.VirtualStorages))
	require.Equal(t, conf.VirtualStorages[0].Name, metadata.GetStorageStatuses()[0].StorageName)
	require.Equal(t, version.GetVersion(), metadata.GetServerVersion())

	gitVersion, err := git.Version()
	require.NoError(t, err)
	require.Equal(t, gitVersion, metadata.GetGitVersion())

	for _, storageStatus := range metadata.GetStorageStatuses() {
		require.NotNil(t, storageStatus, "none of the storage statuses should be nil")
	}
}

func TestGitalyServerInfoBadNode(t *testing.T) {
	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			&config.VirtualStorage{
				Nodes: []*models.Node{
					&models.Node{
						Storage:        "praefect-internal-1",
						Address:        "tcp://unreachable:1234",
						DefaultPrimary: true,
						Token:          "abc",
					},
				},
			},
		},
	}

	entry := testhelper.DiscardTestEntry(t)
	nodeMgr, err := nodes.NewManager(entry, conf, nil, promtest.NewMockHistogramVec())
	require.NoError(t, err)

	registry := protoregistry.New()
	require.NoError(t, registry.RegisterFiles(protoregistry.GitalyProtoFileDescriptors...))

	srv := setupServer(t, conf, nodeMgr, entry, registry)

	listener, port := listenAvailPort(t)
	go func() {
		srv.RegisterServices(nodeMgr, conf, datastore.Datastore{})
		srv.Serve(listener, false)
	}()

	cc := dialLocalPort(t, port, false)
	ctx, cancel := testhelper.Context()
	defer cancel()

	client := gitalypb.NewServerServiceClient(cc)

	metadata, err := client.ServerInfo(ctx, &gitalypb.ServerInfoRequest{})
	require.NoError(t, err)
	require.Len(t, metadata.GetStorageStatuses(), 0)
}

func TestGitalyDiskStatistics(t *testing.T) {
	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Nodes: []*models.Node{
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

	client := healthpb.NewHealthClient(cc)
	_, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
}

func TestRejectBadStorage(t *testing.T) {
	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			&config.VirtualStorage{
				Name: "praefect",
				Nodes: []*models.Node{
					&models.Node{
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
				Nodes: []*models.Node{
					&models.Node{
						DefaultPrimary: true,
						Storage:        "praefect-internal-0",
						Address:        "tcp://abc",
					},
					&models.Node{
						Storage: "praefect-internal-1",
						Address: "tcp://xyz",
					},
				},
			},
			&config.VirtualStorage{
				Name: "praefect",
				Nodes: []*models.Node{
					&models.Node{
						DefaultPrimary: true,
						Storage:        "praefect-internal-0",
						Address:        "tcp://abc",
					},
					&models.Node{
						Storage: "praefect-internal-1",
						Address: "tcp://xyz",
					},
				},
			},
		},
	}

	tLogger, hook := test.NewNullLogger()

	setupServer(t, conf, nil, logrus.NewEntry(tLogger), nil) // instantiates a praefect server and triggers warning

	for _, entry := range hook.Entries {
		require.NotContains(t, entry.Message, "more than one backend node")
	}

	conf = config.Config{
		VirtualStorages: []*config.VirtualStorage{
			&config.VirtualStorage{
				Name: "praefect",
				Nodes: []*models.Node{
					&models.Node{
						DefaultPrimary: true,
						Storage:        "praefect-internal-0",
						Address:        "tcp::/samesies",
					},
					&models.Node{
						Storage: "praefect-internal-1",
						Address: "tcp::/samesies",
					},
				},
			},
		},
	}

	tLogger, hook = test.NewNullLogger()

	setupServer(t, conf, nil, logrus.NewEntry(tLogger), nil) // instantiates a praefect server and triggers warning

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
				Nodes: []*models.Node{
					&models.Node{
						DefaultPrimary: true,
						Storage:        "praefect-internal-0",
						Address:        "tcp://abc",
					},
					&models.Node{
						Storage: "praefect-internal-1",
						Address: "tcp://xyz",
					},
				},
			},
			&config.VirtualStorage{
				Name: "praefect",
				Nodes: []*models.Node{
					&models.Node{
						DefaultPrimary: true,
						Storage:        "praefect-internal-0",
						Address:        "tcp://abc",
					},
					&models.Node{
						Storage: "praefect-internal-2",
						Address: "tcp://xyz",
					},
				},
			},
		},
	}

	tLogger, hook = test.NewNullLogger()

	setupServer(t, conf, nil, logrus.NewEntry(tLogger), nil) // instantiates a praefect server and triggers warning

	for _, entry := range hook.Entries {
		require.NotContains(t, entry.Message, "more than one backend node")
	}
}

func TestRepoRemoval(t *testing.T) {
	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			&config.VirtualStorage{
				Name: "praefect",
				Nodes: []*models.Node{
					&models.Node{
						DefaultPrimary: true,
						Storage:        gconfig.Config.Storages[0].Name,
						Address:        "tcp::/samesies",
					},
					&models.Node{
						Storage: "praefect-internal-1",
						Address: "tcp::/this-doesnt-matter",
					},
					&models.Node{
						Storage: "praefect-internal-2",
						Address: "tcp::/this-doesnt-matter",
					},
				},
			},
		},
	}

	oldStorages := gconfig.Config.Storages
	defer func() { gconfig.Config.Storages = oldStorages }()

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

	cc, _, cleanup := runPraefectServerWithGitaly(t, conf)
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

	// the removal of the repo on the secondary servers is not deterministic
	// since it relies on eventually consistent replication
	pollUntilRemoved(t, path1, time.After(10*time.Second))
	pollUntilRemoved(t, path2, time.After(10*time.Second))
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
				Nodes: []*models.Node{
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

	cc, _, cleanup := runPraefectServerWithGitaly(t, conf)
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
	resp, err = repoServiceClient.RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{
		Repository: renamedVirtualRepo,
	})
	require.NoError(t, err)
	require.True(t, resp.GetExists(), "repo with new name must exist")
	require.DirExists(t, expNewPath0, "must be renamed on secondary from %q to %q", path0, expNewPath0)

	// the renaming of the repo on the secondary servers is not deterministic
	// since it relies on eventually consistent replication
	pollUntilRemoved(t, path1, time.After(10*time.Second))
	require.DirExists(t, expNewPath1, "must be renamed on secondary from %q to %q", path1, expNewPath1)
	pollUntilRemoved(t, path2, time.After(10*time.Second))
	require.DirExists(t, expNewPath2, "must be renamed on secondary from %q to %q", path2, expNewPath2)
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
