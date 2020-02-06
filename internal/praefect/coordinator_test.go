package praefect

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/log"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/proxytime"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/conn"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
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
	clientConnections := conn.NewClientConnections()
	clientConnections.RegisterNode("praefect-internal-1", fmt.Sprintf("tcp://%s", address), "token")

	coordinator := NewCoordinator(log.Default(), ds, clientConnections, conf, proxytime.NewTrailerTracker())
	require.NoError(t, coordinator.RegisterProtos(protoregistry.GitalyProtoFileDescriptors...))

	frame, err := proto.Marshal(&gitalypb.FetchIntoObjectPoolRequest{
		Origin:     &targetRepo,
		ObjectPool: &gitalypb.ObjectPool{Repository: &targetRepo},
		Repack:     false,
	})
	require.NoError(t, err)

	fullMethod := "/gitaly.ObjectPoolService/FetchIntoObjectPool"

	peeker := &mockPeeker{frame}
	streamParams, err := coordinator.streamDirector(ctx, fullMethod, peeker)
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

	jobs, err := ds.GetJobs(datastore.JobStatePending, "praefect-internal-2", 10)
	require.NoError(t, err)
	require.Len(t, jobs, 1)

	targetNode, err := ds.GetStorageNode("praefect-internal-2")
	require.NoError(t, err)
	sourceNode, err := ds.GetStorageNode("praefect-internal-1")

	require.NoError(t, err)

	expectedJob := datastore.ReplJob{
		Change:       datastore.UpdateRepo,
		ID:           1,
		TargetNode:   targetNode,
		SourceNode:   sourceNode,
		State:        datastore.JobStatePending,
		RelativePath: targetRepo.RelativePath,
	}

	require.Equal(t, expectedJob, jobs[0], "ensure replication job created by stream director is correct")

	streamParams.RequestFinalizer()

	jobs, err = coordinator.datastore.GetJobs(datastore.JobStateReady, "praefect-internal-2", 10)
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
