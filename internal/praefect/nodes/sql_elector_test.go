// +build postgres

package nodes

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/promtest"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

var shardName string = "test-shard-0"

func TestGetPrimaryAndSecondaries(t *testing.T) {
	db := getDB(t)

	logger := testhelper.NewTestLogger(t).WithField("test", t.Name())
	praefectSocket := testhelper.GetTemporaryGitalySocketFileName(t)
	socketName := "unix://" + praefectSocket

	conf := config.Config{
		SocketPath: socketName,
		Failover:   config.Failover{Enabled: true},
	}

	internalSocket0 := testhelper.GetTemporaryGitalySocketFileName(t)
	testhelper.NewServerWithHealth(t, internalSocket0)

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

	elector.demotePrimary(ctx, db)
	shard, err := elector.GetShard(ctx)
	db.RequireRowsInTable(t, "shard_primaries", 1)
	require.Equal(t, ErrPrimaryNotHealthy, err)
	require.Empty(t, shard)
}

func TestSqlElector_slow_execution(t *testing.T) {
	db := getDB(t)

	praefectSocket := "unix://" + testhelper.GetTemporaryGitalySocketFileName(t)
	logger := testhelper.NewTestLogger(t).WithField("test", t.Name())

	gitalySocket := testhelper.GetTemporaryGitalySocketFileName(t)
	testhelper.NewServerWithHealth(t, gitalySocket)

	gitalyConn, err := grpc.Dial(
		"unix://"+gitalySocket,
		grpc.WithInsecure(),
	)
	require.NoError(t, err)

	gitalyNodeStatus := newConnectionStatus(config.Node{Storage: "gitaly", Address: "gitaly-address"}, gitalyConn, logger, promtest.NewMockHistogramVec(), nil)
	elector := newSQLElector(shardName, config.Config{SocketPath: praefectSocket}, db.DB, logger, []*nodeStatus{gitalyNodeStatus})

	ctx, cancel := testhelper.Context()
	defer cancel()

	// Failover timeout is set to 0. If the election checks do not happen exactly at the same time
	// as when the health checks are updated, gitaly node in the test is going to be considered
	// unhealthy and the test will fail.
	elector.failoverTimeout = 0

	err = elector.checkNodes(ctx)
	require.NoError(t, err)

	shard, err := elector.GetShard(ctx)
	require.NoError(t, err)
	assertShard(t, shardAssertion{
		Primary:     &nodeAssertion{gitalyNodeStatus.GetStorage(), gitalyNodeStatus.GetAddress()},
		Secondaries: []nodeAssertion{},
	}, shard)
}

func TestBasicFailover(t *testing.T) {
	db := getDB(t)

	logger := testhelper.NewTestLogger(t).WithField("test", t.Name())
	praefectSocket := testhelper.GetTemporaryGitalySocketFileName(t)
	socketName := "unix://" + praefectSocket

	conf := config.Config{SocketPath: socketName}

	internalSocket0, internalSocket1 := testhelper.GetTemporaryGitalySocketFileName(t), testhelper.GetTemporaryGitalySocketFileName(t)
	healthSrv0 := testhelper.NewServerWithHealth(t, internalSocket0)
	healthSrv1 := testhelper.NewServerWithHealth(t, internalSocket1)

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
	shard, err := elector.GetShard(ctx)
	require.NoError(t, err)
	assertShard(t, shardAssertion{
		Primary:     &nodeAssertion{cs0.GetStorage(), cs0.GetAddress()},
		Secondaries: []nodeAssertion{{cs1.GetStorage(), cs1.GetAddress()}},
	}, shard)

	// Bring first node down
	healthSrv0.SetServingStatus("", grpc_health_v1.HealthCheckResponse_UNKNOWN)
	predateElection(t, ctx, db, shardName, failoverTimeout)

	// Primary should remain before the failover timeout is exceeded
	err = elector.checkNodes(ctx)
	require.NoError(t, err)
	shard, err = elector.GetShard(ctx)
	require.NoError(t, err)
	assertShard(t, shardAssertion{
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
	shard, err = elector.GetShard(ctx)
	require.NoError(t, err)
	assertShard(t, shardAssertion{
		Primary:     &nodeAssertion{cs1.GetStorage(), cs1.GetAddress()},
		Secondaries: []nodeAssertion{{cs0.GetStorage(), cs0.GetAddress()}},
	}, shard)

	// Failover back to the original node
	healthSrv0.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	healthSrv1.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	predateElection(t, ctx, db, shardName, failoverTimeout)
	predateLastSeenActiveAt(t, db, shardName, cs1.GetStorage(), failoverTimeout)
	require.NoError(t, elector.checkNodes(ctx))

	shard, err = elector.GetShard(ctx)
	require.NoError(t, err)
	assertShard(t, shardAssertion{
		Primary:     &nodeAssertion{cs0.GetStorage(), cs0.GetAddress()},
		Secondaries: []nodeAssertion{{cs1.GetStorage(), cs1.GetAddress()}},
	}, shard)

	// Bring second node down
	healthSrv0.SetServingStatus("", grpc_health_v1.HealthCheckResponse_UNKNOWN)
	predateElection(t, ctx, db, shardName, failoverTimeout)
	predateLastSeenActiveAt(t, db, shardName, cs0.GetStorage(), failoverTimeout)

	err = elector.checkNodes(ctx)
	require.NoError(t, err)
	db.RequireRowsInTable(t, "node_status", 2)
	// No new candidates
	_, err = elector.GetShard(ctx)
	require.Equal(t, ErrPrimaryNotHealthy, err)
}

func TestElectDemotedPrimary(t *testing.T) {
	db := getDB(t)

	tx, err := db.Begin()
	require.NoError(t, err)
	defer func() { require.NoError(t, tx.Commit()) }()

	node := config.Node{Storage: "gitaly-0"}
	elector := newSQLElector(
		shardName,
		config.Config{},
		db.DB,
		testhelper.DiscardTestLogger(t),
		[]*nodeStatus{{node: node}},
	)

	ctx, cancel := testhelper.Context()
	defer cancel()

	candidates := []*sqlCandidate{{Node: &nodeStatus{node: node}}}
	require.NoError(t, elector.electNewPrimary(ctx, tx, candidates))

	primary, err := elector.lookupPrimary(ctx, tx)
	require.NoError(t, err)
	require.Equal(t, node.Storage, primary.GetStorage())

	require.NoError(t, elector.demotePrimary(ctx, tx))

	primary, err = elector.lookupPrimary(ctx, tx)
	require.NoError(t, err)
	require.Nil(t, primary)

	predateElection(t, ctx, tx, shardName, failoverTimeout+time.Microsecond)
	require.NoError(t, err)
	require.NoError(t, elector.electNewPrimary(ctx, tx, candidates))

	primary, err = elector.lookupPrimary(ctx, tx)
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
func predateElection(t testing.TB, ctx context.Context, db glsql.Querier, shardName string, amount time.Duration) {
	t.Helper()

	_, err := db.ExecContext(ctx,
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
		fallbackChoice         bool
	}{
		{
			desc: "gitaly-2 storage has more up to date repositories",
			initialReplQueueInsert: `
			INSERT INTO repositories
				(virtual_storage, relative_path, generation)
			VALUES
				('test-shard-0', '/p/1', 5),
				('test-shard-0', '/p/2', 5),
				('test-shard-0', '/p/3', 5),
				('test-shard-0', '/p/4', 5),
				('test-shard-0', '/p/5', 5);

			INSERT INTO storage_repositories
				(virtual_storage, relative_path, storage, generation)
			VALUES
				('test-shard-0', '/p/1', 'gitaly-1', 5),
				('test-shard-0', '/p/2', 'gitaly-1', 5),
				('test-shard-0', '/p/3', 'gitaly-1', 4),
				('test-shard-0', '/p/4', 'gitaly-1', 3),

				('test-shard-0', '/p/1', 'gitaly-2', 5),
				('test-shard-0', '/p/2', 'gitaly-2', 5),
				('test-shard-0', '/p/3', 'gitaly-2', 4),
				('test-shard-0', '/p/4', 'gitaly-2', 4),
				('test-shard-0', '/p/5', 'gitaly-2', 5)
			`,
			expectedPrimary: "gitaly-2",
			fallbackChoice:  false,
		},
		{
			desc: "gitaly-2 storage has less repositories as some may not been replicated yet",
			initialReplQueueInsert: `
			INSERT INTO REPOSITORIES
				(virtual_storage, relative_path, generation)
			VALUES
				('test-shard-0', '/p/1', 5),
				('test-shard-0', '/p/2', 5);

			INSERT INTO STORAGE_REPOSITORIES
			VALUES
				('test-shard-0', '/p/1', 'gitaly-1', 5),
				('test-shard-0', '/p/2', 'gitaly-1', 4),
				('test-shard-0', '/p/1', 'gitaly-2', 5)`,
			expectedPrimary: "gitaly-1",
			fallbackChoice:  false,
		},
		{
			desc: "gitaly-1 is primary as it has less generations behind in total despite it has less repositories",
			initialReplQueueInsert: `
			INSERT INTO REPOSITORIES
				(virtual_storage, relative_path, generation)
			VALUES
				('test-shard-0', '/p/1', 2),
				('test-shard-0', '/p/2', 2),
				('test-shard-0', '/p/3', 10);

			INSERT INTO STORAGE_REPOSITORIES
			VALUES
				('test-shard-0', '/p/2', 'gitaly-1', 1),
				('test-shard-0', '/p/3', 'gitaly-1', 9),
				('test-shard-0', '/p/1', 'gitaly-2', 1),
				('test-shard-0', '/p/2', 'gitaly-2', 1),
				('test-shard-0', '/p/3', 'gitaly-2', 1)`,
			expectedPrimary: "gitaly-1",
			fallbackChoice:  false,
		},
		{
			desc:            "no information about generations results to first candidate",
			expectedPrimary: "gitaly-1",
			fallbackChoice:  true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			db.TruncateAll(t)

			tx, err := db.Begin()
			require.NoError(t, err)
			defer func() { require.NoError(t, tx.Commit()) }()

			_, err = tx.Exec(testCase.initialReplQueueInsert)
			require.NoError(t, err)

			logger, hook := test.NewNullLogger()

			elector := newSQLElector(shardName, config.Config{}, db.DB, logger, ns)

			ctx, cancel := testhelper.Context()
			defer cancel()

			require.NoError(t, elector.electNewPrimary(ctx, tx, candidates))
			primary, err := elector.lookupPrimary(ctx, tx)

			require.NoError(t, err)
			require.Equal(t, testCase.expectedPrimary, primary.GetStorage())

			fallbackChoice := hook.LastEntry().Data["fallback_choice"].(bool)
			require.Equal(t, testCase.fallbackChoice, fallbackChoice)
		})
	}
}

func TestConnectionMultiplexing(t *testing.T) {
	errNonMuxed := status.Error(codes.Internal, "non-muxed connection")
	errMuxed := status.Error(codes.Internal, "muxed connection")

	logger := testhelper.DiscardTestEntry(t)

	srv := grpc.NewServer(
		grpc.Creds(backchannel.NewServerHandshaker(logger, backchannel.Insecure(), backchannel.NewRegistry(), nil)),
		grpc.UnknownServiceHandler(func(srv interface{}, stream grpc.ServerStream) error {
			_, err := backchannel.GetPeerID(stream.Context())
			if err == backchannel.ErrNonMultiplexedConnection {
				return errNonMuxed
			}

			assert.NoError(t, err)
			return errMuxed
		}),
	)

	grpc_health_v1.RegisterHealthServer(srv, health.NewServer())

	defer srv.Stop()

	ln, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	go srv.Serve(ln)

	mgr, err := NewManager(
		testhelper.DiscardTestEntry(t),
		config.Config{
			Failover: config.Failover{
				Enabled:          true,
				ElectionStrategy: config.ElectionStrategySQL,
			},
			VirtualStorages: []*config.VirtualStorage{
				{
					Name: "virtual-storage-1",
					Nodes: []*config.Node{
						{Storage: "storage-1", Address: "tcp://" + ln.Addr().String()},
						{Storage: "storage-2", Address: "tcp://" + ln.Addr().String()},
					},
				},
			},
		},
		getDB(t).DB,
		nil,
		promtest.NewMockHistogramVec(),
		protoregistry.GitalyProtoPreregistered,
		nil,
		backchannel.NewClientHandshaker(logger, func() backchannel.Server { return grpc.NewServer() }),
	)

	// check the shard to get the primary in a healthy state
	mgr.checkShards()

	ctx, cancel := testhelper.Context()
	defer cancel()
	for _, tc := range []struct {
		desc  string
		error error
	}{
		{
			desc:  "multiplexed",
			error: errMuxed,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			shard, err := mgr.GetShard(ctx, "virtual-storage-1")
			require.NoError(t, err)
			require.Len(t, shard.Secondaries, 1)

			for _, node := range []Node{shard.Primary, shard.Secondaries[0]} {
				require.Equal(t,
					tc.error,
					node.GetConnection().Invoke(ctx, "/Service/Method", &gitalypb.VoteTransactionRequest{}, &gitalypb.VoteTransactionResponse{}),
				)
			}

			nodes := mgr.Nodes()["virtual-storage-1"]
			require.Len(t, nodes, 2)
			for _, node := range nodes {
				require.Equal(t,
					tc.error,
					node.GetConnection().Invoke(ctx, "/Service/Method", &gitalypb.VoteTransactionRequest{}, &gitalypb.VoteTransactionResponse{}),
				)
			}
		})
	}
}
