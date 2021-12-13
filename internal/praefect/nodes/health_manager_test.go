package nodes

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

type mockHealthClient struct {
	grpc_health_v1.HealthClient
	CheckFunc func(context.Context, *grpc_health_v1.HealthCheckRequest, ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error)
}

func (m mockHealthClient) Check(ctx context.Context, r *grpc_health_v1.HealthCheckRequest, opts ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
	return m.CheckFunc(ctx, r, opts...)
}

func getHealthConsensus(ctx context.Context, t *testing.T, db glsql.Querier) map[string][]string {
	t.Helper()

	rows, err := db.QueryContext(ctx, "SELECT virtual_storage, storage FROM healthy_storages")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	consensus := map[string][]string{}
	for rows.Next() {
		var virtualStorage, storage string
		require.NoError(t, rows.Scan(&virtualStorage, &storage))

		storages := consensus[virtualStorage]
		storages = append(storages, storage)
		consensus[virtualStorage] = storages
	}

	require.NoError(t, rows.Err())
	return consensus
}

func TestHealthManager(t *testing.T) {
	t.Parallel()
	ctx, cancel := testhelper.Context()
	defer cancel()

	type LocalStatus map[string]map[string]bool

	type HealthChecks []struct {
		After           time.Duration
		PraefectName    string
		LocalStatus     LocalStatus
		HealthConsensus map[string][]string
	}

	db := testdb.NewDB(t)

	for _, tc := range []struct {
		desc         string
		healthChecks HealthChecks
	}{
		{
			desc: "single voter basic scenarios",
			healthChecks: HealthChecks{
				{
					PraefectName: "praefect-1",
					LocalStatus: LocalStatus{
						"virtual-storage-1": {
							"gitaly-1": true,
							"gitaly-2": true,
						},
						"virtual-storage-2": {
							"gitaly-1": true,
							"gitaly-2": false,
						},
						"virtual-storage-3": {
							"gitaly-1": false,
							"gitlay-2": false,
						},
					},
					HealthConsensus: map[string][]string{
						"virtual-storage-1": {"gitaly-1", "gitaly-2"},
						"virtual-storage-2": {"gitaly-1"},
					},
				},
			},
		},
		{
			desc: "updates own vote to healthy",
			healthChecks: HealthChecks{
				{
					PraefectName: "praefect-1",
					LocalStatus: LocalStatus{
						"virtual-storage-1": {
							"gitaly-1": false,
						},
					},
					HealthConsensus: map[string][]string{},
				},
				{
					PraefectName: "praefect-1",
					LocalStatus: LocalStatus{
						"virtual-storage-1": {
							"gitaly-1": true,
						},
					},
					HealthConsensus: map[string][]string{
						"virtual-storage-1": {"gitaly-1"},
					},
				},
			},
		},
		{
			desc: "counts own healthy vote before timeout",
			healthChecks: HealthChecks{
				{
					PraefectName: "praefect-1",
					LocalStatus: LocalStatus{
						"virtual-storage-1": {
							"gitaly-1": true,
						},
					},
					HealthConsensus: map[string][]string{
						"virtual-storage-1": {"gitaly-1"},
					},
				},
				{
					PraefectName: "praefect-1",
					LocalStatus: LocalStatus{
						"virtual-storage-1": {
							"gitaly-1": false,
						},
					},
					HealthConsensus: map[string][]string{
						"virtual-storage-1": {"gitaly-1"},
					},
				},
			},
		},
		{
			desc: "discounts own healthy vote after timeout",
			healthChecks: HealthChecks{
				{
					PraefectName: "praefect-1",
					LocalStatus: LocalStatus{
						"virtual-storage-1": {
							"gitaly-1": true,
						},
					},
					HealthConsensus: map[string][]string{
						"virtual-storage-1": {"gitaly-1"},
					},
				},
				{
					After:        failoverTimeout,
					PraefectName: "praefect-1",
					LocalStatus: LocalStatus{
						"virtual-storage-1": {
							"gitaly-1": false,
						},
					},
					HealthConsensus: map[string][]string{},
				},
			},
		},
		{
			desc: "inactive praefects not part of quorum",
			healthChecks: HealthChecks{
				{
					PraefectName: "praefect-1",
					LocalStatus: LocalStatus{
						"virtual-storage-1": {
							"gitaly-1": false,
						},
					},
					HealthConsensus: map[string][]string{},
				},
				{
					PraefectName: "praefect-2",
					LocalStatus: LocalStatus{
						"virtual-storage-1": {
							"gitaly-1": false,
						},
					},
					HealthConsensus: map[string][]string{},
				},
				{
					After:        activePraefectTimeout,
					PraefectName: "praefect-3",
					LocalStatus: LocalStatus{
						"virtual-storage-1": {
							"gitaly-1": true,
						},
					},
					HealthConsensus: map[string][]string{
						"virtual-storage-1": {"gitaly-1"},
					},
				},
			},
		},
		{
			desc: "unconfigured node in minority is unhealthy",
			healthChecks: HealthChecks{
				{
					PraefectName: "praefect-1",
					LocalStatus: LocalStatus{
						"virtual-storage-1": {
							"configured": true,
						},
					},
					HealthConsensus: map[string][]string{
						"virtual-storage-1": {"configured"},
					},
				},
				{
					PraefectName: "praefect-2",
					LocalStatus: LocalStatus{
						"virtual-storage-1": {
							"configured": true,
						},
					},
					HealthConsensus: map[string][]string{
						"virtual-storage-1": {"configured"},
					},
				},
				{
					PraefectName: "praefect-3",
					LocalStatus: LocalStatus{
						"virtual-storage-1": {
							"configured":   true,
							"unconfigured": true,
						},
					},
					HealthConsensus: map[string][]string{
						"virtual-storage-1": {"configured"},
					},
				},
			},
		},
		{
			desc: "unconfigured node in majority is unhealthy",
			healthChecks: HealthChecks{
				{
					PraefectName: "praefect-1",
					LocalStatus: LocalStatus{
						"virtual-storage-1": {
							"configured":   true,
							"unconfigured": true,
						},
					},
					HealthConsensus: map[string][]string{
						"virtual-storage-1": {"configured", "unconfigured"},
					},
				},
				{
					PraefectName: "praefect-2",
					LocalStatus: LocalStatus{
						"virtual-storage-1": {
							"configured":   true,
							"unconfigured": true,
						},
					},
					HealthConsensus: map[string][]string{
						"virtual-storage-1": {"configured", "unconfigured"},
					},
				},
				{
					PraefectName: "praefect-3",
					LocalStatus: LocalStatus{
						"virtual-storage-1": {
							"configured": true,
						},
					},
					HealthConsensus: map[string][]string{
						"virtual-storage-1": {"configured", "unconfigured"},
					},
				},
			},
		},
		{
			desc: "majority consensus healthy",
			healthChecks: HealthChecks{
				{
					PraefectName: "praefect-1",
					LocalStatus: LocalStatus{
						"virtual-storage-1": {
							"gitaly-1": false,
						},
					},
					HealthConsensus: map[string][]string{},
				},
				{
					PraefectName: "praefect-2",
					LocalStatus: LocalStatus{
						"virtual-storage-1": {
							"gitaly-1": true,
						},
					},
					HealthConsensus: map[string][]string{
						"virtual-storage-1": {"gitaly-1"},
					},
				},
				{
					PraefectName: "praefect-3",
					LocalStatus: LocalStatus{
						"virtual-storage-1": {
							"gitaly-1": true,
						},
					},
					HealthConsensus: map[string][]string{
						"virtual-storage-1": {"gitaly-1"},
					},
				},
			},
		},
		{
			desc: "majority consensus unhealthy",
			healthChecks: HealthChecks{
				{
					PraefectName: "praefect-1",
					LocalStatus: LocalStatus{
						"virtual-storage-1": {
							"gitaly-1": true,
						},
					},
					HealthConsensus: map[string][]string{
						"virtual-storage-1": {"gitaly-1"},
					},
				},
				{
					PraefectName: "praefect-2",
					LocalStatus: LocalStatus{
						"virtual-storage-1": {
							"gitaly-1": false,
						},
					},
					HealthConsensus: map[string][]string{
						"virtual-storage-1": {"gitaly-1"},
					},
				},
				{
					PraefectName: "praefect-3",
					LocalStatus: LocalStatus{
						"virtual-storage-1": {
							"gitaly-1": false,
						},
					},
					HealthConsensus: map[string][]string{},
				},
			},
		},
		{
			desc: "first check triggers update even if all nodes are unhealthy",
			healthChecks: HealthChecks{
				{
					PraefectName: "praefect-1",
					LocalStatus: LocalStatus{
						"virtual-storage-1": {
							"gitaly-1": false,
						},
					},
					HealthConsensus: map[string][]string{},
				},
				{
					PraefectName: "praefect-1",
					LocalStatus: LocalStatus{
						"virtual-storage-1": {
							"gitaly-1": false,
						},
					},
					HealthConsensus: map[string][]string{},
				},
			},
		},
		{
			desc: "node becoming healthy is reflected in the consensus",
			healthChecks: HealthChecks{
				{
					PraefectName: "praefect-1",
					LocalStatus: LocalStatus{
						"virtual-storage-1": {
							"gitaly-1": true,
							"gitaly-2": false,
						},
					},
					HealthConsensus: map[string][]string{
						"virtual-storage-1": {"gitaly-1"},
					},
				},
				{
					PraefectName: "praefect-1",
					LocalStatus: LocalStatus{
						"virtual-storage-1": {
							"gitaly-1": true,
							"gitaly-2": true,
						},
					},
					HealthConsensus: map[string][]string{
						"virtual-storage-1": {"gitaly-1", "gitaly-2"},
					},
				},
			},
		},
		{
			desc: "same health check status does not change consensus",
			healthChecks: HealthChecks{
				{
					PraefectName: "praefect-1",
					LocalStatus: LocalStatus{
						"virtual-storage-1": {
							"gitaly-1": true,
						},
					},
					HealthConsensus: map[string][]string{
						"virtual-storage-1": {"gitaly-1"},
					},
				},
				{
					PraefectName: "praefect-1",
					LocalStatus: LocalStatus{
						"virtual-storage-1": {
							"gitaly-1": true,
						},
					},
					HealthConsensus: map[string][]string{
						"virtual-storage-1": {"gitaly-1"},
					},
				},
			},
		},
		{
			desc: "nodes changing health status changes health consensus",
			healthChecks: HealthChecks{
				{
					PraefectName: "praefect-1",
					LocalStatus: LocalStatus{
						"virtual-storage-1": {
							"gitaly-1": true,
							"gitaly-2": false,
						},
					},
					HealthConsensus: map[string][]string{
						"virtual-storage-1": {"gitaly-1"},
					},
				},
				{
					After:        failoverTimeout,
					PraefectName: "praefect-1",
					LocalStatus: LocalStatus{
						"virtual-storage-1": {
							"gitaly-1": false,
							"gitaly-2": true,
						},
					},
					HealthConsensus: map[string][]string{
						"virtual-storage-1": {"gitaly-2"},
					},
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			db.TruncateAll(t)

			healthStatus := map[string]grpc_health_v1.HealthCheckResponse_ServingStatus{}
			// healthManagers are cached in order to keep the internal state intact between different
			// health checks during the test.
			healthManagers := map[string]*HealthManager{}

			hasUpdated := map[string]bool{}

			for i, hc := range tc.healthChecks {
				// Create or use existing health managers
				hm, ok := healthManagers[hc.PraefectName]
				if !ok {
					clients := make(HealthClients, len(hc.LocalStatus))
					for virtualStorage, nodeHealths := range hc.LocalStatus {
						clients[virtualStorage] = make(map[string]grpc_health_v1.HealthClient, len(nodeHealths))
						for node := range nodeHealths {
							virtualStorage, node := virtualStorage, node
							clients[virtualStorage][node] = mockHealthClient{
								CheckFunc: func(context.Context, *grpc_health_v1.HealthCheckRequest, ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
									return &grpc_health_v1.HealthCheckResponse{Status: healthStatus[virtualStorage+node]}, nil
								},
							}
						}
					}

					hm = NewHealthManager(testhelper.NewDiscardingLogger(t), db, hc.PraefectName, clients)
					hm.handleError = func(err error) error { return err }
					healthManagers[hc.PraefectName] = hm
				}

				// Set health statuses to the expected
				for virtualStorage, nodeHealths := range hc.LocalStatus {
					for node, healthy := range nodeHealths {
						status := grpc_health_v1.HealthCheckResponse_UNKNOWN
						if healthy {
							status = grpc_health_v1.HealthCheckResponse_SERVING
						}

						healthStatus[virtualStorage+node] = status
					}
				}

				// predate earlier health checks to simulate this health check being run after a certain
				// time period
				if hc.After > 0 {
					predateHealthChecks(t, db, hc.After)
				}

				expectedHealthyNodes := map[string][]string{}
				for virtualStorage, storages := range hc.LocalStatus {
					for storage, healthy := range storages {
						if !healthy {
							continue
						}

						expectedHealthyNodes[virtualStorage] = append(expectedHealthyNodes[virtualStorage], storage)
					}

					sort.Strings(expectedHealthyNodes[virtualStorage])
				}

				runCtx, cancelRun := context.WithCancel(ctx)
				require.Equal(t, context.Canceled, hm.Run(runCtx, helper.NewCountTicker(1, cancelRun)))

				// we need to sort the storages so the require.Equal matches, ElementsMatch does not work with a map.
				actualHealthyNodes := hm.HealthyNodes()
				for _, storages := range actualHealthyNodes {
					sort.Strings(storages)
				}

				require.Equal(t, expectedHealthyNodes, actualHealthyNodes, "health check %d", i+1)
				require.Equal(t, hc.HealthConsensus, getHealthConsensus(ctx, t, db), "health check %d", i+1)

				select {
				case <-hm.Updated():
					require.False(t, hasUpdated[hc.PraefectName], "Updated should tick on the first update to signal readiness")
				default:
					require.True(t, hasUpdated[hc.PraefectName], "Updated should only tick on the first update to signal readiness")
				}

				hasUpdated[hc.PraefectName] = true
			}
		})
	}
}

func TestHealthManager_databaseTimeout(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	db := testdb.NewDB(t)

	blockingTx := db.Begin(t)
	defer blockingTx.Rollback(t)

	newHealthManager := func(db glsql.Querier) *HealthManager {
		return NewHealthManager(testhelper.NewDiscardingLogger(t), db, "praefect", HealthClients{
			"virtual-storage": {
				"gitaly": mockHealthClient{
					CheckFunc: func(context.Context, *grpc_health_v1.HealthCheckRequest, ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
						return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
					},
				},
			},
		})
	}

	// Run an update and leave the transaction open to block the other client.
	blockingMgr := newHealthManager(blockingTx)
	runCtx, cancelRun := context.WithCancel(ctx)
	require.Equal(t, context.Canceled, blockingMgr.Run(runCtx, helper.NewCountTicker(1, cancelRun)))

	blockedMgr := newHealthManager(db)

	var timeoutQuery func()
	blockedMgr.databaseTimeout = func(ctx context.Context) (context.Context, func()) {
		ctx, timeoutQuery = context.WithCancel(ctx)
		return ctx, timeoutQuery
	}
	blockedMgr.handleError = func(err error) error { return err }

	blockedErr := make(chan error)
	// This will block in database waiting for a lock.
	go func() {
		blockedErr <- blockedMgr.Run(ctx, helper.NewCountTicker(1, func() {}))
	}()

	// Wait until the blocked query is waiting.
	testdb.WaitForBlockedQuery(ctx, t, db, "INSERT INTO node_status")
	// Simulate a timeout.
	timeoutQuery()
	// Query should have been canceled.
	require.EqualError(t, <-blockedErr, "update checks: pq: canceling statement due to user request")
}

func predateHealthChecks(t testing.TB, db testdb.DB, amount time.Duration) {
	t.Helper()

	_, err := db.Exec(`
		UPDATE node_status SET
			last_contact_attempt_at = last_contact_attempt_at - INTERVAL '1 MICROSECOND' * $1,
			last_seen_active_at = last_seen_active_at - INTERVAL '1 MICROSECOND' * $1
		`, amount.Microseconds(),
	)
	require.NoError(t, err)
}

// This test case ensures the record updates are done in an ordered manner to avoid concurrent writes
// deadlocking. Issue: https://gitlab.com/gitlab-org/gitaly/-/issues/3907
func TestHealthManager_orderedWrites(t *testing.T) {
	db := testdb.NewDB(t)

	tx1 := db.Begin(t).Tx
	defer func() { _ = tx1.Rollback() }()

	tx2 := db.Begin(t).Tx
	defer func() { _ = tx2.Rollback() }()

	ctx, cancel := testhelper.Context()
	defer cancel()

	const (
		praefectName   = "praefect-1"
		virtualStorage = "virtual-storage"
	)

	returnErr := func(err error) error { return err }

	hm1 := NewHealthManager(testhelper.NewDiscardingLogger(t), tx1, praefectName, nil)
	hm1.handleError = returnErr
	require.NoError(t, hm1.updateHealthChecks(ctx, []string{virtualStorage}, []string{"gitaly-1"}, []bool{true}))

	tx2Err := make(chan error, 1)
	hm2 := NewHealthManager(testhelper.NewDiscardingLogger(t), tx2, praefectName, nil)
	hm2.handleError = returnErr
	go func() {
		tx2Err <- hm2.updateHealthChecks(ctx, []string{virtualStorage, virtualStorage}, []string{"gitaly-2", "gitaly-1"}, []bool{true, true})
	}()

	// Wait for tx2 to be blocked on the gitaly-1 lock acquired by tx1
	testdb.WaitForBlockedQuery(ctx, t, db, "INSERT INTO node_status")

	// Ensure tx1 can acquire lock on gitaly-2.
	require.NoError(t, hm1.updateHealthChecks(ctx, []string{virtualStorage}, []string{"gitaly-2"}, []bool{true}))
	// Committing tx1 releases locks and unblocks tx2.
	require.NoError(t, tx1.Commit())

	// tx2 should succeed afterwards.
	require.NoError(t, <-tx2Err)
	require.NoError(t, tx2.Commit())

	require.Equal(t, map[string][]string{"virtual-storage": {"gitaly-1", "gitaly-2"}}, getHealthConsensus(ctx, t, db))
}
