// +build postgres

package nodes

import (
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/promtest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

var shardName string = "test-shard-0"

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
	cs0 := newConnectionStatus(config.Node{Storage: storageName + "-0"}, cc0, testhelper.DiscardTestEntry(t), mockHistogramVec0, nil)

	ns := []*nodeStatus{cs0}
	elector := newSQLElector(shardName, conf, db.DB, logger, ns)
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

	addr0 := "unix://" + internalSocket0
	cc0, err := grpc.Dial(
		addr0,
		grpc.WithInsecure(),
	)
	require.NoError(t, err)

	addr1 := "unix://" + internalSocket1
	cc1, err := grpc.Dial(
		addr1,
		grpc.WithInsecure(),
	)

	require.NoError(t, err)

	storageName := "default"

	cs0 := newConnectionStatus(config.Node{Storage: storageName + "-0", Address: addr0}, cc0, logger, promtest.NewMockHistogramVec(), nil)
	cs1 := newConnectionStatus(config.Node{Storage: storageName + "-1", Address: addr1}, cc1, logger, promtest.NewMockHistogramVec(), nil)

	ns := []*nodeStatus{cs0, cs1}
	elector := newSQLElector(shardName, conf, db.DB, logger, ns)

	ctx, cancel := testhelper.Context()
	defer cancel()
	err = elector.checkNodes(ctx)

	require.NoError(t, err)
	db.RequireRowsInTable(t, "node_status", 2)
	db.RequireRowsInTable(t, "shard_primaries", 1)

	require.Equal(t, cs0, elector.primaryNode.Node)
	shard, err := elector.GetShard()
	require.NoError(t, err)
	assertShard(t, shardAssertion{
		IsReadOnly:  false,
		Primary:     &nodeAssertion{cs0.GetStorage(), cs0.GetAddress()},
		Secondaries: []nodeAssertion{{cs1.GetStorage(), cs1.GetAddress()}},
	}, shard)

	// Bring first node down
	healthSrv0.SetServingStatus("", grpc_health_v1.HealthCheckResponse_UNKNOWN)
	predateElection(t, db, shardName, failoverTimeout)

	// Primary should remain before the failover timeout is exceeded
	err = elector.checkNodes(ctx)
	require.NoError(t, err)
	shard, err = elector.GetShard()
	require.NoError(t, err)
	assertShard(t, shardAssertion{
		IsReadOnly:  false,
		Primary:     &nodeAssertion{cs0.GetStorage(), cs0.GetAddress()},
		Secondaries: []nodeAssertion{{cs1.GetStorage(), cs1.GetAddress()}},
	}, shard)

	// Predate the timeout to exceed it
	predateLastSeenActiveAt(t, db, shardName, cs0.GetStorage(), failoverTimeout)

	// Expect that the other node is promoted
	err = elector.checkNodes(ctx)
	require.NoError(t, err)

	db.RequireRowsInTable(t, "node_status", 2)
	db.RequireRowsInTable(t, "shard_primaries", 1)
	shard, err = elector.GetShard()
	require.NoError(t, err)
	assertShard(t, shardAssertion{
		// previous primary was write-enabled, so we should record it
		PreviousWritablePrimary: &nodeAssertion{cs0.GetStorage(), cs0.GetAddress()},
		IsReadOnly:              true,
		Primary:                 &nodeAssertion{cs1.GetStorage(), cs1.GetAddress()},
		Secondaries:             []nodeAssertion{{cs0.GetStorage(), cs0.GetAddress()}},
	}, shard)

	// Failover back to the original node
	healthSrv0.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	healthSrv1.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	predateElection(t, db, shardName, failoverTimeout)
	predateLastSeenActiveAt(t, db, shardName, cs1.GetStorage(), failoverTimeout)
	require.NoError(t, elector.checkNodes(ctx))

	shard, err = elector.GetShard()
	require.NoError(t, err)
	assertShard(t, shardAssertion{
		// failing back to the original node means the primary is the same as the
		// previous write-enabled primary. Node 2 was read-only, so field is not
		// modified
		PreviousWritablePrimary: &nodeAssertion{cs0.GetStorage(), cs0.GetAddress()},
		IsReadOnly:              true,
		Primary:                 &nodeAssertion{cs0.GetStorage(), cs0.GetAddress()},
		Secondaries:             []nodeAssertion{{cs1.GetStorage(), cs1.GetAddress()}},
	}, shard)

	// We should be able to enable writes on the new primary
	require.NoError(t, elector.enableWrites(ctx))
	shard, err = elector.GetShard()
	require.NoError(t, err)
	assertShard(t, shardAssertion{
		PreviousWritablePrimary: &nodeAssertion{cs0.GetStorage(), cs0.GetAddress()},
		IsReadOnly:              false,
		Primary:                 &nodeAssertion{cs0.GetStorage(), cs0.GetAddress()},
		Secondaries:             []nodeAssertion{{cs1.GetStorage(), cs1.GetAddress()}},
	}, shard)

	// Bring second node down
	healthSrv0.SetServingStatus("", grpc_health_v1.HealthCheckResponse_UNKNOWN)

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

	node := config.Node{Storage: "gitaly-0"}
	elector := newSQLElector(
		shardName,
		config.Config{},
		db.DB,
		testhelper.DiscardTestLogger(t),
		[]*nodeStatus{{node: node}},
	)

	candidates := []*sqlCandidate{{Node: &nodeStatus{node: node}}}
	require.NoError(t, elector.electNewPrimary(candidates))

	primary, _, _, err := elector.lookupPrimary()
	require.NoError(t, err)
	require.Equal(t, node.Storage, primary.GetStorage())

	require.NoError(t, elector.demotePrimary())

	primary, _, _, err = elector.lookupPrimary()
	require.NoError(t, err)
	require.Nil(t, primary)

	predateElection(t, db, shardName, failoverTimeout)
	require.NoError(t, err)
	require.NoError(t, elector.electNewPrimary(candidates))

	primary, _, _, err = elector.lookupPrimary()
	require.NoError(t, err)
	require.Equal(t, node.Storage, primary.GetStorage())
}

// predateLastSeenActiveAt shifts the last_seen_active_at column to an earlier time. This avoids
// waiting for the node's status to become unhealthy.
func predateLastSeenActiveAt(t testing.TB, db glsql.DB, shardName, nodeName string, amount time.Duration) {
	t.Helper()

	_, err := db.Exec(`
UPDATE node_status SET last_seen_active_at = last_seen_active_at - INTERVAL '1 MICROSECOND' * $1
WHERE shard_name = $2 AND node_name = $3`, amount.Microseconds(), shardName, nodeName,
	)

	require.NoError(t, err)
}

// predateElection shifts the election to an earlier time. This avoids waiting for the failover timeout to trigger
// a new election.
func predateElection(t testing.TB, db glsql.DB, shardName string, amount time.Duration) {
	t.Helper()

	_, err := db.Exec(
		"UPDATE shard_primaries SET elected_at = elected_at - INTERVAL '1 MICROSECOND' * $1 WHERE shard_name = $2",
		amount.Microseconds(),
		shardName,
	)
	require.NoError(t, err)
}

func TestElectNewPrimary(t *testing.T) {
	db := getDB(t)

	ns := []*nodeStatus{{
		node: config.Node{
			Storage: "gitaly-0",
		},
	}, {
		node: config.Node{
			Storage: "gitaly-1",
		},
	}, {
		node: config.Node{
			Storage: "gitaly-2",
		},
	}}

	candidates := []*sqlCandidate{
		{
			&nodeStatus{
				node: config.Node{
					Storage: "gitaly-1",
				},
			},
		}, {
			&nodeStatus{
				node: config.Node{
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
			elector := newSQLElector(shardName, conf, db.DB, testhelper.DiscardTestLogger(t), ns)

			require.NoError(t, elector.electNewPrimary(candidates))
			primary, readOnly, _, err := elector.lookupPrimary()
			require.NoError(t, err)
			require.Equal(t, "gitaly-1", primary.GetStorage(), "since replication queue is empty the first candidate should be chosen")
			require.False(t, readOnly)

			predateElection(t, db, shardName, failoverTimeout)
			_, err = db.Exec(testCase.initialReplQueueInsert)
			require.NoError(t, err)

			logger, hook := test.NewNullLogger()
			logger.SetFormatter(&logrus.JSONFormatter{})

			elector.log = logger
			require.NoError(t, elector.electNewPrimary(candidates))

			primary, readOnly, _, err = elector.lookupPrimary()
			require.NoError(t, err)
			require.Equal(t, testCase.expectedPrimary, primary.GetStorage())
			require.True(t, readOnly)

			incompleteCounts := hook.LastEntry().Data["incomplete_counts"].([]targetNodeIncompleteCounts)
			require.Equal(t, testCase.incompleteCounts, incompleteCounts)
			require.Equal(t, testCase.expectedPrimary, hook.LastEntry().Data["new_primary"].(string))
		})
	}
}
