package praefect

import (
	"context"
	"io/ioutil"
	"sync"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/metadatahandler"
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

	var replEventWait sync.WaitGroup

	queueInterceptor := datastore.NewReplicationEventQueueInterceptor(datastore.NewMemoryReplicationEventQueue())
	queueInterceptor.OnEnqueue(func(ctx context.Context, event datastore.ReplicationEvent, queue datastore.ReplicationEventQueue) (datastore.ReplicationEvent, error) {
		defer replEventWait.Done()
		return queue.Enqueue(ctx, event)
	})

	ds := datastore.Datastore{
		ReplicasDatastore:     datastore.NewInMemory(conf),
		ReplicationEventQueue: queueInterceptor,
	}

	targetRepo := gitalypb.Repository{
		StorageName:  "praefect",
		RelativePath: "/path/to/hashed/storage",
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	address := "gitaly-primary.example.com"

	entry := testhelper.DiscardTestEntry(t)

	nodeMgr, err := nodes.NewManager(entry, conf, nil, promtest.NewMockHistogramVec())
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

	replEventWait.Add(1) // expected only one event to be created
	// this call creates new events in the queue and simulates usual flow of the update operation
	streamParams.RequestFinalizer()

	targetNode, err := ds.GetStorageNode("praefect-internal-2")
	require.NoError(t, err)
	sourceNode, err := ds.GetStorageNode("praefect-internal-1")
	require.NoError(t, err)

	replEventWait.Wait() // wait until event persisted (async operation)
	events, err := ds.ReplicationEventQueue.Dequeue(ctx, "praefect-internal-2", 10)
	require.NoError(t, err)
	require.Len(t, events, 1)

	expectedEvent := datastore.ReplicationEvent{
		ID:        1,
		State:     datastore.JobStateInProgress,
		Attempt:   2,
		LockID:    "praefect-internal-2|/path/to/hashed/storage",
		CreatedAt: events[0].CreatedAt,
		UpdatedAt: events[0].UpdatedAt,
		Job: datastore.ReplicationJob{
			Change:            datastore.UpdateRepo,
			RelativePath:      targetRepo.RelativePath,
			TargetNodeStorage: targetNode.Storage,
			SourceNodeStorage: sourceNode.Storage,
		},
		Meta: datastore.Params{metadatahandler.CorrelationIDKey: "my-correlation-id"},
	}
	require.Equal(t, expectedEvent, events[0], "ensure replication job created by stream director is correct")
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

	var replEventWait sync.WaitGroup

	queueInterceptor := datastore.NewReplicationEventQueueInterceptor(datastore.NewMemoryReplicationEventQueue())
	queueInterceptor.OnEnqueue(func(ctx context.Context, event datastore.ReplicationEvent, queue datastore.ReplicationEventQueue) (datastore.ReplicationEvent, error) {
		defer replEventWait.Done()
		return queue.Enqueue(ctx, event)
	})

	ds := datastore.Datastore{
		ReplicasDatastore:     datastore.NewInMemory(conf),
		ReplicationEventQueue: queueInterceptor,
	}

	targetRepo := gitalypb.Repository{
		StorageName:  "praefect",
		RelativePath: "/path/to/hashed/storage",
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	address := "gitaly-primary.example.com"

	entry := testhelper.DiscardTestEntry(t)

	nodeMgr, err := nodes.NewManager(entry, conf, nil, promtest.NewMockHistogramVec())
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

	replEventWait.Add(1) // expected only one event to be created
	// must be run as it adds replication events to the queue
	streamParams.RequestFinalizer()

	replEventWait.Wait() // wait until event persisted (async operation)
	jobs, err := coordinator.datastore.Dequeue(ctx, conf.VirtualStorages[0].Nodes[1].Storage, 1)
	require.NoError(t, err)
	require.Len(t, jobs, 1)

	require.NotZero(t, jobs[0].Meta[metadatahandler.CorrelationIDKey],
		"the coordinator should have generated a random ID")
}
