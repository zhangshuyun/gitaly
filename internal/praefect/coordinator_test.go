package praefect

import (
	"io/ioutil"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/promtest"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/labkit/correlation"
)

var testLogger = logrus.New()

func init() {
	testLogger.SetOutput(ioutil.Discard)
}

func TestSecondaryRotation(t *testing.T) {
	t.Skip("secondary rotation will change with the new data model")
}

func TestStreamDirector(t *testing.T) {
	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			&config.VirtualStorage{
				Name: "praefect",
				Nodes: []*models.Node{
					&models.Node{
						Address:        "tcp://gitaly-primary.example.com",
						Storage:        "praefect-internal-1",
						DefaultPrimary: true,
					},
					&models.Node{
						Address: "tcp://gitaly-backup1.example.com",
						Storage: "praefect-internal-2",
					}},
			},
		},
	}
	ds := datastore.NewInMemory(conf)

	targetRepo := gitalypb.Repository{
		StorageName:  "praefect",
		RelativePath: "/path/to/hashed/storage",
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	address := "gitaly-primary.example.com"

	entry := testhelper.DiscardTestEntry(t)

	nodeMgr, err := nodes.NewManager(entry, conf, promtest.NewMockHistogramVec())
	require.NoError(t, err)
	r := protoregistry.New()
	require.NoError(t, r.RegisterFiles(protoregistry.GitalyProtoFileDescriptors...))

	coordinator := NewCoordinator(entry, ds, nodeMgr, conf, r)

	frame, err := proto.Marshal(&gitalypb.FetchIntoObjectPoolRequest{
		Origin:     &targetRepo,
		ObjectPool: &gitalypb.ObjectPool{Repository: &targetRepo},
		Repack:     false,
	})
	require.NoError(t, err)

	fullMethod := "/gitaly.ObjectPoolService/FetchIntoObjectPool"

	peeker := &mockPeeker{frame}
	streamParams, err := coordinator.StreamDirector(correlation.ContextWithCorrelation(ctx, "my-correlation-id"), fullMethod, peeker)
	require.NoError(t, err)
	require.Equal(t, address, streamParams.Conn().Target())

	mi, err := coordinator.registry.LookupMethod(fullMethod)
	require.NoError(t, err)

	m, err := protoMessageFromPeeker(mi, peeker)
	require.NoError(t, err)

	rewrittenTargetRepo, err := mi.TargetRepo(m)
	require.NoError(t, err)
	require.Equal(t, "praefect-internal-1", rewrittenTargetRepo.GetStorageName(), "stream director should have rewritten the storage name")

	rewrittenRepo, err := mi.TargetRepo(m)
	require.NoError(t, err)
	require.Equal(t, "praefect-internal-1", rewrittenRepo.GetStorageName(), "stream director should have rewritten the storage name")

	jobs, err := ds.GetJobs([]datastore.JobState{datastore.JobStatePending}, "praefect-internal-2", 10)
	require.NoError(t, err)
	require.Len(t, jobs, 1)

	targetNode, err := ds.GetStorageNode("praefect-internal-2")
	require.NoError(t, err)
	sourceNode, err := ds.GetStorageNode("praefect-internal-1")

	require.NoError(t, err)

	expectedJob := datastore.ReplJob{
		Change:        datastore.UpdateRepo,
		ID:            1,
		TargetNode:    targetNode,
		SourceNode:    sourceNode,
		State:         datastore.JobStatePending,
		RelativePath:  targetRepo.RelativePath,
		CorrelationID: "my-correlation-id",
	}

	require.Equal(t, expectedJob, jobs[0], "ensure replication job created by stream director is correct")

	streamParams.RequestFinalizer()

	jobs, err = coordinator.datastore.GetJobs([]datastore.JobState{datastore.JobStateReady}, "praefect-internal-2", 10)
	require.NoError(t, err)
	require.Len(t, jobs, 1)

	expectedJob.State = datastore.JobStateReady
	require.Equal(t, expectedJob, jobs[0], "ensure replication job's status has been updatd to JobStateReady")
}

type mockPeeker struct {
	frame []byte
}

func (m *mockPeeker) Peek() ([]byte, error) {
	return m.frame, nil
}

func (m *mockPeeker) Modify(payload []byte) error {
	m.frame = payload

	return nil
}

func TestAbsentCorrelationID(t *testing.T) {
	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			&config.VirtualStorage{
				Name: "praefect",
				Nodes: []*models.Node{
					&models.Node{
						Address:        "tcp://gitaly-primary.example.com",
						Storage:        "praefect-internal-1",
						DefaultPrimary: true,
					},
					&models.Node{
						Address: "tcp://gitaly-backup1.example.com",
						Storage: "praefect-internal-2",
					}},
			},
		},
	}
	ds := datastore.NewInMemory(conf)

	targetRepo := gitalypb.Repository{
		StorageName:  "praefect",
		RelativePath: "/path/to/hashed/storage",
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	address := "gitaly-primary.example.com"

	entry := testhelper.DiscardTestEntry(t)

	nodeMgr, err := nodes.NewManager(entry, conf, promtest.NewMockHistogramVec())
	require.NoError(t, err)

	coordinator := NewCoordinator(entry, ds, nodeMgr, conf, protoregistry.GitalyProtoPreregistered)

	frame, err := proto.Marshal(&gitalypb.FetchIntoObjectPoolRequest{
		Origin:     &targetRepo,
		ObjectPool: &gitalypb.ObjectPool{Repository: &targetRepo},
		Repack:     false,
	})
	require.NoError(t, err)

	fullMethod := "/gitaly.ObjectPoolService/FetchIntoObjectPool"
	peeker := &mockPeeker{frame}
	streamParams, err := coordinator.StreamDirector(ctx, fullMethod, peeker)
	require.NoError(t, err)
	require.Equal(t, address, streamParams.Conn().Target())

	jobs, err := coordinator.datastore.GetJobs([]datastore.JobState{datastore.JobStatePending}, conf.VirtualStorages[0].Nodes[1].Storage, 1)
	require.NoError(t, err)
	require.Len(t, jobs, 1)

	require.NotZero(t, jobs[0].CorrelationID,
		"the coordinator should have generated a random ID")
}

func TestStreamDirector_SkipReplication(t *testing.T) {
	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			&config.VirtualStorage{
				Name: "praefect",
				Nodes: []*models.Node{
					&models.Node{
						Address:        "tcp://gitaly-primary.example.com",
						Storage:        "default",
						DefaultPrimary: true,
					},
					&models.Node{
						Address: "tcp://gitaly-backup1.example.com",
						Storage: "backup",
					}},
			},
		},
	}

	logEntry := testhelper.DiscardTestEntry(t)
	ds := datastore.NewInMemory(conf)

	nodeMgr, err := nodes.NewManager(testhelper.DiscardTestEntry(t), conf, promtest.NewMockHistogramVec())
	require.NoError(t, err)

	registry := protoregistry.New()
	require.NoError(t, registry.RegisterFiles(protoregistry.GitalyProtoFileDescriptors...))

	coordinator := NewCoordinator(logEntry, ds, nodeMgr, conf, registry)

	prf := NewServer(
		coordinator.StreamDirector,
		logEntry,
		registry,
		conf,
	)
	defer prf.Stop()

	listener, port := listenAvailPort(t)
	go prf.Serve(listener, false)

	ctx, cancel := testhelper.Context()
	defer cancel()

	repositoryClient := gitalypb.NewRepositoryServiceClient(dialLocalPort(t, port, false))

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()
	testRepo.StorageName = "praefect"

	// since we have no gitaly backends configured, these requests will all fail.
	repositoryClient.GarbageCollect(ctx, &gitalypb.GarbageCollectRequest{
		Repository: testRepo,
	})
	repositoryClient.RepackFull(ctx, &gitalypb.RepackFullRequest{
		Repository: testRepo,
	})
	repositoryClient.RepackIncremental(ctx, &gitalypb.RepackIncrementalRequest{
		Repository: testRepo,
	})

	jobs, err := ds.GetJobs([]datastore.JobState{datastore.JobStateReady}, "backup", 10)
	require.NoError(t, err)
	require.Len(t, jobs, 0)

	repositoryClient.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{
		Repository: testRepo,
	})

	jobs, err = ds.GetJobs([]datastore.JobState{datastore.JobStateReady}, "backup", 10)
	require.NoError(t, err)
	require.Len(t, jobs, 1)

	// since the above requests failed, the jobs should be in JobStatePending rather than
	// JobStateReady. This is a bug and is tracked via https://gitlab.com/gitlab-org/gitaly/-/issues/2566
}
