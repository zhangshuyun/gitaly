// +build postgres

package nodes

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/promtest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

var shardName string = "test-shard-0"

func TestMetrics(t *testing.T) {
	db := getDB(t)

	// set up copied from below
	const virtualStorageName = "virtual-storage-0"
	const primaryStorage = "praefect-internal-0"
	socket0, socket1 := testhelper.GetTemporaryGitalySocketFileName(), testhelper.GetTemporaryGitalySocketFileName()
	virtualStorage := &config.VirtualStorage{
		Name: virtualStorageName,
		Nodes: []*models.Node{
			{
				Storage:        primaryStorage,
				Address:        "unix://" + socket0,
				DefaultPrimary: true,
			},
			{
				Storage: "praefect-internal-1",
				Address: "unix://" + socket1,
			},
		},
	}

	srv0, _ := testhelper.NewServerWithHealth(t, socket0)
	defer srv0.Stop()

	srv1, _ := testhelper.NewServerWithHealth(t, socket1)
	defer srv1.Stop()

	conf := config.Config{
		Failover:        config.Failover{Enabled: true, ElectionStrategy: "sql"},
		VirtualStorages: []*config.VirtualStorage{virtualStorage},
	}
	nm, err := NewManager(testhelper.DiscardTestEntry(t), conf, db.DB, datastore.Datastore{}, promtest.NewMockHistogramVec())
	require.NoError(t, err)

	ctx, cancel := testhelper.Context()
	defer cancel()
	err = nm.strategies[virtualStorageName].checkNodes(ctx)
	db.RequireRowsInTable(t, "shard_primaries", 1)

	// the actual test code
	expected := strings.NewReader(`
# HELP gitaly_get_shard_total Total count of GetShard calls
# TYPE gitaly_get_shard_total counter
gitaly_get_shard_total{virtual_storage="virtual-storage-0"} 2

# HELP gitaly_never_set_gauge This is never touched, yet is visible in scrape
# TYPE gitaly_never_set_gauge gauge
gitaly_never_set_gauge{virtual_storage="virtual-storage-0"} 0

# HELP gitaly_praefect_primaries Primary/Secondary statuses of Gitaly nodes.
# TYPE gitaly_praefect_primaries gauge
gitaly_praefect_primaries{gitaly_storage="praefect-internal-0",virtual_storage="virtual-storage-0"} 1
gitaly_praefect_primaries{gitaly_storage="praefect-internal-1",virtual_storage="virtual-storage-0"} 0
`)

	// Multiple tests can now use the metrics as the metrics are per elector instance.
	// The metrics are not registered in the tests, so there won't be duplicate registrations
	// in the global registry. Alternatively, if the test does not care about metrics, it can
	// just completely ignore them without having to provide mock metrics.
	require.NoError(t, testutil.CollectAndCompare(nm, expected))
}

func TestGetPrimaryAndSecondaries(t *testing.T) {
	db := getDB(t)

	logger := testhelper.NewTestLogger(t).WithField("test", t.Name())
	praefectSocket := testhelper.GetTemporaryGitalySocketFileName()
	socketName := "unix://" + praefectSocket

	conf := config.Config{
		SocketPath: socketName,
	}

	internalSocket0 := testhelper.GetTemporaryGitalySocketFileName()
	srv0, _ := testhelper.NewServerWithHealth(t, internalSocket0)
	defer srv0.Stop()

	cc0, err := grpc.Dial(
		"unix://"+internalSocket0,
		grpc.WithInsecure(),
	)
	require.NoError(t, err)

	storageName := "default"
	mockHistogramVec0 := promtest.NewMockHistogramVec()
	cs0 := newConnectionStatus(models.Node{Storage: storageName + "-0"}, cc0, testhelper.DiscardTestEntry(t), mockHistogramVec0)

	ns := []*nodeStatus{cs0}
	elector := newSQLElector(shardName, conf, 1, defaultActivePraefectSeconds, db.DB, logger, ns)
	require.Contains(t, elector.praefectName, ":"+socketName)
	require.Equal(t, elector.shardName, shardName)

	ctx, cancel := testhelper.Context()
	defer cancel()
	err = elector.checkNodes(ctx)
	db.RequireRowsInTable(t, "shard_primaries", 1)

	elector.demotePrimary()
	shard, err := elector.GetShard()
	db.RequireRowsInTable(t, "shard_primaries", 1)
	require.Equal(t, ErrPrimaryNotHealthy, err)
	require.Empty(t, shard)
}

func TestBasicFailover(t *testing.T) {
	db := getDB(t)

	logger := testhelper.NewTestLogger(t).WithField("test", t.Name())
	praefectSocket := testhelper.GetTemporaryGitalySocketFileName()
	socketName := "unix://" + praefectSocket

	conf := config.Config{
		SocketPath: socketName,
		Failover: config.Failover{
			ReadOnlyAfterFailover: true,
		},
	}

	internalSocket0, internalSocket1 := testhelper.GetTemporaryGitalySocketFileName(), testhelper.GetTemporaryGitalySocketFileName()
	srv0, healthSrv0 := testhelper.NewServerWithHealth(t, internalSocket0)
	defer srv0.Stop()

	srv1, healthSrv1 := testhelper.NewServerWithHealth(t, internalSocket1)
	defer srv1.Stop()

	cc0, err := grpc.Dial(
		"unix://"+internalSocket0,
		grpc.WithInsecure(),
	)
	require.NoError(t, err)

	cc1, err := grpc.Dial(
		"unix://"+internalSocket1,
		grpc.WithInsecure(),
	)

	require.NoError(t, err)

	storageName := "default"
	mockHistogramVec0, mockHistogramVec1 := promtest.NewMockHistogramVec(), promtest.NewMockHistogramVec()
	cs0 := newConnectionStatus(models.Node{Storage: storageName + "-0"}, cc0, testhelper.DiscardTestEntry(t), mockHistogramVec0)
	cs1 := newConnectionStatus(models.Node{Storage: storageName + "-1"}, cc1, testhelper.DiscardTestEntry(t), mockHistogramVec1)

	ns := []*nodeStatus{cs0, cs1}
	failoverTimeSeconds := 1
	elector := newSQLElector(shardName, conf, failoverTimeSeconds, defaultActivePraefectSeconds, db.DB, logger, ns)

	ctx, cancel := testhelper.Context()
	defer cancel()
	err = elector.checkNodes(ctx)

	require.NoError(t, err)
	db.RequireRowsInTable(t, "node_status", 2)
	db.RequireRowsInTable(t, "shard_primaries", 1)

	require.Equal(t, cs0, elector.primaryNode.Node)
	shard, err := elector.GetShard()
	require.NoError(t, err)
	require.Equal(t, cs0.GetStorage(), shard.Primary.GetStorage())
	require.Equal(t, 1, len(shard.Secondaries))
	require.Equal(t, cs1.GetStorage(), shard.Secondaries[0].GetStorage())
	require.False(t, shard.IsReadOnly, "new shard should not be read-only")

	// Bring first node down
	healthSrv0.SetServingStatus("", grpc_health_v1.HealthCheckResponse_UNKNOWN)

	// pretend like the last election happened in the past because the query in electNewPrimary to update the primary will not
	// overwrite a previous that's within 10 seconds
	_, err = db.Exec(`UPDATE shard_primaries SET elected_at = now() - $1::INTERVAL SECOND WHERE shard_name = $2`,
		2*failoverTimeSeconds,
		shardName)
	require.NoError(t, err)

	// Primary should remain even after the first check
	err = elector.checkNodes(ctx)
	require.NoError(t, err)
	shard, err = elector.GetShard()
	require.NoError(t, err)
	require.False(t, shard.IsReadOnly)

	// Wait for stale timeout to expire
	time.Sleep(1 * time.Second)

	// Expect that the other node is promoted
	err = elector.checkNodes(ctx)
	require.NoError(t, err)

	db.RequireRowsInTable(t, "node_status", 2)
	db.RequireRowsInTable(t, "shard_primaries", 1)
	shard, err = elector.GetShard()
	require.NoError(t, err)
	require.Equal(t, cs1.GetStorage(), shard.Primary.GetStorage())
	require.True(t, shard.IsReadOnly, "shard should be read-only after a failover")

	// We should be able to enable writes on the new primary
	require.NoError(t, elector.enableWrites(ctx))
	shard, err = elector.GetShard()
	require.NoError(t, err)
	require.False(t, shard.IsReadOnly, "")

	// Bring second node down
	healthSrv1.SetServingStatus("", grpc_health_v1.HealthCheckResponse_UNKNOWN)

	// Wait for stale timeout to expire
	time.Sleep(1 * time.Second)
	err = elector.checkNodes(ctx)
	require.NoError(t, err)

	db.RequireRowsInTable(t, "node_status", 2)
	// No new candidates
	_, err = elector.GetShard()
	require.Error(t, ErrPrimaryNotHealthy, err)
	require.Error(t, ErrPrimaryNotHealthy, elector.enableWrites(ctx),
		"shouldn't be able to enable writes with unhealthy master")
}

func TestElectDemotedPrimary(t *testing.T) {
	db := getDB(t)

	node := models.Node{Storage: "gitaly-0"}
	elector := newSQLElector(
		shardName,
		config.Config{},
		defaultFailoverTimeoutSeconds,
		defaultActivePraefectSeconds,
		db.DB,
		testhelper.DiscardTestLogger(t),
		[]*nodeStatus{{Node: node}},
	)

	candidates := []*sqlCandidate{{Node: &nodeStatus{Node: node}}}
	require.NoError(t, elector.electNewPrimary(candidates))

	primary, _, err := elector.lookupPrimary()
	require.NoError(t, err)
	require.Equal(t, node.Storage, primary.GetStorage())

	require.NoError(t, elector.demotePrimary())

	primary, _, err = elector.lookupPrimary()
	require.NoError(t, err)
	require.Nil(t, primary)

	shiftElection(t, db, shardName, -1*defaultFailoverTimeoutSeconds)
	require.NoError(t, err)
	require.NoError(t, elector.electNewPrimary(candidates))

	primary, _, err = elector.lookupPrimary()
	require.NoError(t, err)
	require.Equal(t, node.Storage, primary.GetStorage())
}

// pretend like the last election happened in the past because the query in electNewPrimary
// to update the primary will not overwrite a previous that's within 10 seconds
func shiftElection(t testing.TB, db glsql.DB, shardName string, seconds int) {
	t.Helper()

	_, err := db.Exec(
		"UPDATE shard_primaries SET elected_at = elected_at + $1::INTERVAL SECOND WHERE shard_name = $2",
		seconds,
		shardName,
	)
	require.NoError(t, err)
}

func TestElectNewPrimary(t *testing.T) {
	db := getDB(t)

	ns := []*nodeStatus{{
		Node: models.Node{
			Storage:        "gitaly-0",
			DefaultPrimary: true,
		},
	}, {
		Node: models.Node{
			Storage:        "gitaly-1",
			DefaultPrimary: true,
		},
	}, {
		Node: models.Node{
			Storage:        "gitaly-2",
			DefaultPrimary: true,
		},
	}}

	failoverTimeSeconds := 1
	candidates := []*sqlCandidate{
		{
			&nodeStatus{
				Node: models.Node{
					Storage: "gitaly-1",
				},
			},
		}, {
			&nodeStatus{
				Node: models.Node{
					Storage: "gitaly-2",
				},
			},
		}}

	testCases := []struct {
		desc                   string
		initialReplQueueInsert string
		expectedPrimary        string
		incompleteCounts       []targetNodeIncompleteCounts
	}{{
		desc: "gitaly-1's most recent job status after the last completion is a dead job",
		initialReplQueueInsert: `INSERT INTO replication_queue
	(job, updated_at, state)
	VALUES
	('{"virtual_storage": "test-shard-1", "target_node_storage": "gitaly-1"}', '2020-01-01 00:00:05', 'ready'),
	('{"virtual_storage": "test-shard-1", "target_node_storage": "gitaly-2"}', '2020-01-01 00:00:05', 'completed'),

	('{"virtual_storage": "test-shard-0", "target_node_storage": "gitaly-1"}', '2020-01-01 00:00:04', 'dead'),
	('{"virtual_storage": "test-shard-0", "target_node_storage": "gitaly-1"}', '2020-01-01 00:00:03', 'completed'),
	('{"virtual_storage": "test-shard-0", "target_node_storage": "gitaly-1"}', '2020-01-01 00:00:02', 'completed'),
	('{"virtual_storage": "test-shard-0", "target_node_storage": "gitaly-1"}', '2020-01-01 00:00:01', 'completed'),
	('{"virtual_storage": "test-shard-0", "target_node_storage": "gitaly-1"}', '2020-01-01 00:00:00', 'completed'),

	('{"virtual_storage": "test-shard-0", "target_node_storage": "gitaly-2"}', '2020-01-01 00:00:04', 'completed'),
	('{"virtual_storage": "test-shard-0", "target_node_storage": "gitaly-2"}', '2020-01-01 00:00:03', 'dead'),
	('{"virtual_storage": "test-shard-0", "target_node_storage": "gitaly-2"}', '2020-01-01 00:00:02', 'dead'),
	('{"virtual_storage": "test-shard-0", "target_node_storage": "gitaly-2"}', '2020-01-01 00:00:01', 'dead'),
	('{"virtual_storage": "test-shard-0", "target_node_storage": "gitaly-2"}', '2020-01-01 00:00:00', 'dead')`,
		expectedPrimary: "gitaly-2",
		incompleteCounts: []targetNodeIncompleteCounts{
			{
				NodeStorage: "gitaly-2",
				Ready:       0,
				InProgress:  0,
				Failed:      0,
				Dead:        0,
			},
			{
				NodeStorage: "gitaly-1",
				Ready:       0,
				InProgress:  0,
				Failed:      0,
				Dead:        1,
			},
		},
	},
		{
			desc: "gitaly-1 has 2 dead jobs, while gitaly-2 has ready and in_progress jobs",
			initialReplQueueInsert: `INSERT INTO replication_queue
		(job, updated_at, state)
		VALUES
		('{"virtual_storage": "test-shard-1", "target_node_storage": "gitaly-1"}', '2020-01-01 00:00:05', 'in_progress'),
		('{"virtual_storage": "test-shard-1", "target_node_storage": "gitaly-2"}', '2020-01-01 00:00:05', 'completed'),

		('{"virtual_storage": "test-shard-0", "target_node_storage": "gitaly-1"}', '2020-01-01 00:00:02', 'dead'),
		('{"virtual_storage": "test-shard-0", "target_node_storage": "gitaly-1"}', '2020-01-01 00:00:01', 'dead'),
		('{"virtual_storage": "test-shard-0", "target_node_storage": "gitaly-1"}', '2020-01-01 00:00:00', 'completed'),

		('{"virtual_storage": "test-shard-0", "target_node_storage": "gitaly-2"}', '2020-01-01 00:00:03', 'ready'),
		('{"virtual_storage": "test-shard-0", "target_node_storage": "gitaly-2"}', '2020-01-01 00:00:02', 'in_progress'),
		('{"virtual_storage": "test-shard-0", "target_node_storage": "gitaly-2"}', '2020-01-01 00:00:01', 'ready'),
		('{"virtual_storage": "test-shard-0", "target_node_storage": "gitaly-2"}', '2020-01-01 00:00:00', 'completed')`,
			expectedPrimary: "gitaly-2",
			incompleteCounts: []targetNodeIncompleteCounts{
				{
					NodeStorage: "gitaly-2",
					Ready:       2,
					InProgress:  1,
					Failed:      0,
					Dead:        0,
				},
				{
					NodeStorage: "gitaly-1",
					Ready:       0,
					InProgress:  0,
					Failed:      0,
					Dead:        2,
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			db.TruncateAll(t)

			conf := config.Config{Failover: config.Failover{ReadOnlyAfterFailover: true}}
			elector := newSQLElector(shardName, conf, failoverTimeSeconds, defaultActivePraefectSeconds, db.DB, testhelper.DiscardTestLogger(t), ns)

			require.NoError(t, elector.electNewPrimary(candidates))
			primary, readOnly, err := elector.lookupPrimary()
			require.NoError(t, err)
			require.Equal(t, "gitaly-1", primary.GetStorage(), "since replication queue is empty the first candidate should be chosen")
			require.False(t, readOnly)

			shiftElection(t, db, shardName, -1*failoverTimeSeconds)
			_, err = db.Exec(testCase.initialReplQueueInsert)
			require.NoError(t, err)

			logger, hook := test.NewNullLogger()
			logger.SetFormatter(&logrus.JSONFormatter{})

			elector.log = logger
			require.NoError(t, elector.electNewPrimary(candidates))

			primary, readOnly, err = elector.lookupPrimary()
			require.NoError(t, err)
			require.Equal(t, testCase.expectedPrimary, primary.GetStorage())
			require.True(t, readOnly)

			incompleteCounts := hook.LastEntry().Data["incomplete_counts"].([]targetNodeIncompleteCounts)
			require.Equal(t, testCase.incompleteCounts, incompleteCounts)
			require.Equal(t, testCase.expectedPrimary, hook.LastEntry().Data["new_primary"].(string))
		})
	}
}
