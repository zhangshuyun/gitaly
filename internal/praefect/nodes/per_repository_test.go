// +build postgres

package nodes

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/commonerr"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func setHealthyNodes(t testing.TB, ctx context.Context, db glsql.Querier, healthyNodes map[string]map[string][]string) {
	var praefects, virtualStorages, storages []string
	for praefect, virtualStors := range healthyNodes {
		for virtualStorage, stors := range virtualStors {
			for _, storage := range stors {
				praefects = append(praefects, praefect)
				virtualStorages = append(virtualStorages, virtualStorage)
				storages = append(storages, storage)
			}
		}
	}

	_, err := db.ExecContext(ctx, `
WITH clear_previous_checks AS ( DELETE FROM node_status )

INSERT INTO node_status (praefect_name, shard_name, node_name, last_contact_attempt_at, last_seen_active_at)
SELECT
	unnest($1::text[]) AS praefect_name,
	unnest($2::text[]) AS shard_name,
	unnest($3::text[]) AS node_name,
	NOW() AS last_contact_attempt_at,
	NOW() AS last_seen_active_at
ON CONFLICT (praefect_name, shard_name, node_name) DO UPDATE SET
	last_contact_attempt_at = NOW(),
	last_seen_active_at = NOW()
		`,
		pq.StringArray(praefects),
		pq.StringArray(virtualStorages),
		pq.StringArray(storages),
	)
	require.NoError(t, err)
}

func TestPerRepositoryElector(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	type storageRecord struct {
		generation int
		assigned   bool
	}

	type state map[string]map[string]map[string]storageRecord

	type matcher func(t testing.TB, primary string)
	any := func(expected ...string) matcher {
		return func(t testing.TB, primary string) {
			t.Helper()
			require.Contains(t, expected, primary)
		}
	}

	noPrimary := func() matcher {
		return func(t testing.TB, primary string) {
			t.Helper()
			require.Empty(t, primary)
		}
	}

	type logMatcher func(t testing.TB, entry logrus.Entry)

	anyChange := func(expected ...primaryChanges) logMatcher {
		return func(t testing.TB, entry logrus.Entry) {
			require.Equal(t, "performed failovers", entry.Message)

			var fields []logrus.Fields
			for _, changes := range expected {
				fields = append(fields, logrus.Fields{
					"component": "PerRepositoryElector",
					"changes":   changes,
				})
			}

			require.Contains(t, fields, entry.Data)
		}
	}

	noChanges := func(t testing.TB, entry logrus.Entry) {
		t.Helper()
		require.Equal(t, "attempting failovers resulted no changes", entry.Message)
	}

	type steps []struct {
		healthyNodes map[string][]string
		error        error
		primary      matcher
		matchLogs    logMatcher
	}

	for _, tc := range []struct {
		desc         string
		state        state
		steps        steps
		existingJobs []datastore.ReplicationEvent
	}{
		{
			desc: "elects the most up to date storage",
			state: state{
				"virtual-storage-1": {
					"relative-path-1": {
						"gitaly-1": {generation: 1},
						"gitaly-2": {generation: 0},
					},
				},
			},
			steps: steps{
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-1", "gitaly-2", "gitaly-3"},
					},
					primary:   any("gitaly-1"),
					matchLogs: anyChange(primaryChanges{"virtual-storage-1": {"gitaly-1": {"demoted": 0, "promoted": 1}}}),
				},
			},
		},
		{
			desc: "does not elect healthy outdated replicas",
			state: state{
				"virtual-storage-1": {
					"relative-path-1": {
						"gitaly-1": {generation: 1},
						"gitaly-2": {generation: 0},
					},
				},
			},
			steps: steps{
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-2", "gitaly-3"},
					},
					error:     ErrNoPrimary,
					primary:   noPrimary(),
					matchLogs: noChanges,
				},
			},
		},
		{
			desc: "no valid primary",
			state: state{
				"virtual-storage-1": {
					"relative-path-1": {
						"gitaly-1": {generation: 0},
					},
				},
			},
			steps: steps{
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-2", "gitaly-3"},
					},
					error:     ErrNoPrimary,
					primary:   noPrimary(),
					matchLogs: noChanges,
				},
			},
		},
		{
			desc: "random healthy node on the latest generation",
			state: state{
				"virtual-storage-1": {
					"relative-path-1": {
						"gitaly-1": {generation: 0},
						"gitaly-2": {generation: 0},
					},
				},
			},
			steps: steps{
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-1", "gitaly-2", "gitaly-3"},
					},
					primary: any("gitaly-1", "gitaly-2"),
					matchLogs: anyChange(
						primaryChanges{"virtual-storage-1": {"gitaly-1": {"demoted": 0, "promoted": 1}}},
						primaryChanges{"virtual-storage-1": {"gitaly-2": {"demoted": 0, "promoted": 1}}},
					),
				},
			},
		},
		{
			desc: "fails over to up to date healthy note",
			state: state{
				"virtual-storage-1": {
					"relative-path-1": {
						"gitaly-1": {generation: 1},
						"gitaly-2": {generation: 1},
						"gitaly-3": {generation: 0},
					},
				},
			},
			steps: steps{
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-1", "gitaly-3"},
					},
					primary:   any("gitaly-1"),
					matchLogs: anyChange(primaryChanges{"virtual-storage-1": {"gitaly-1": {"demoted": 0, "promoted": 1}}}),
				},
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-2", "gitaly-3"},
					},
					primary: any("gitaly-2"),
					matchLogs: anyChange(primaryChanges{"virtual-storage-1": {
						"gitaly-1": {"demoted": 1, "promoted": 0},
						"gitaly-2": {"demoted": 0, "promoted": 1},
					}}),
				},
			},
		},
		{
			desc: "does not fail over to healthy outdated nodes",
			state: state{
				"virtual-storage-1": {
					"relative-path-1": {
						"gitaly-1": {generation: 1},
						"gitaly-3": {generation: 0},
					},
				},
			},
			steps: steps{
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-1", "gitaly-2", "gitaly-3"},
					},
					primary:   any("gitaly-1"),
					matchLogs: anyChange(primaryChanges{"virtual-storage-1": {"gitaly-1": {"demoted": 0, "promoted": 1}}}),
				},
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-2", "gitaly-3"},
					},
					error:     ErrNoPrimary,
					primary:   noPrimary(),
					matchLogs: anyChange(primaryChanges{"virtual-storage-1": {"gitaly-1": {"demoted": 1, "promoted": 0}}}),
				},
			},
		},
		{
			desc: "fails over to assigned nodes when assignments are set",
			state: state{
				"virtual-storage-1": {
					"relative-path-1": {
						"gitaly-1": {generation: 2, assigned: true},
						"gitaly-2": {generation: 2, assigned: true},
						"gitaly-3": {generation: 2, assigned: false},
					},
				},
			},
			steps: steps{
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-1", "gitaly-3"},
					},
					primary:   any("gitaly-1"),
					matchLogs: anyChange(primaryChanges{"virtual-storage-1": {"gitaly-1": {"demoted": 0, "promoted": 1}}}),
				},
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-2", "gitaly-3"},
					},
					primary: any("gitaly-2"),
					matchLogs: anyChange(primaryChanges{"virtual-storage-1": {
						"gitaly-1": {"demoted": 1, "promoted": 0},
						"gitaly-2": {"demoted": 0, "promoted": 1},
					}}),
				},
			},
		},
		{
			desc: "fails over to unassigned replicas if no valid assigned primaries exist",
			state: state{
				"virtual-storage-1": {
					"relative-path-1": {
						"gitaly-1": {generation: 2, assigned: true},
						"gitaly-2": {generation: 2, assigned: false},
					},
				},
			},
			steps: steps{
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-1", "gitaly-2", "gitaly-3"},
					},
					primary:   any("gitaly-1"),
					matchLogs: anyChange(primaryChanges{"virtual-storage-1": {"gitaly-1": {"demoted": 0, "promoted": 1}}}),
				},
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-2", "gitaly-3"},
					},
					primary: any("gitaly-2"),
					matchLogs: anyChange(primaryChanges{"virtual-storage-1": {
						"gitaly-1": {"demoted": 1, "promoted": 0},
						"gitaly-2": {"demoted": 0, "promoted": 1},
					}}),
				},
			},
		},
		{
			desc: "fails over to up to date assigned replica from healthy unassigned",
			state: state{
				"virtual-storage-1": {
					"relative-path-1": {
						"gitaly-1": {generation: 2, assigned: true},
						"gitaly-2": {generation: 2, assigned: false},
					},
				},
			},
			steps: steps{
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-2", "gitaly-3"},
					},
					primary:   any("gitaly-2"),
					matchLogs: anyChange(primaryChanges{"virtual-storage-1": {"gitaly-2": {"demoted": 0, "promoted": 1}}}),
				},
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-1", "gitaly-2", "gitaly-3"},
					},
					primary: any("gitaly-1"),
					matchLogs: anyChange(primaryChanges{"virtual-storage-1": {
						"gitaly-1": {"demoted": 0, "promoted": 1},
						"gitaly-2": {"demoted": 1, "promoted": 0},
					}}),
				},
			},
		},
		{
			desc: "doesnt fail over to outdated assigned replica from healthy unassigned",
			state: state{
				"virtual-storage-1": {
					"relative-path-1": {
						"gitaly-1": {generation: 1, assigned: true},
						"gitaly-2": {generation: 2, assigned: false},
					},
				},
			},
			steps: steps{
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-2", "gitaly-3"},
					},
					primary:   any("gitaly-2"),
					matchLogs: anyChange(primaryChanges{"virtual-storage-1": {"gitaly-2": {"demoted": 0, "promoted": 1}}}),
				},
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-1", "gitaly-2", "gitaly-3"},
					},
					primary:   any("gitaly-2"),
					matchLogs: noChanges,
				},
			},
		},
		{
			desc: "demotes the primary when there are no valid candidates",
			state: state{
				"virtual-storage-1": {
					"relative-path-1": {
						"gitaly-1": {generation: 1, assigned: true},
						"gitaly-2": {generation: 0, assigned: false},
					},
				},
			},
			steps: steps{
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-1", "gitaly-2", "gitaly-3"},
					},
					primary:   any("gitaly-1"),
					matchLogs: anyChange(primaryChanges{"virtual-storage-1": {"gitaly-1": {"demoted": 0, "promoted": 1}}}),
				},
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-2", "gitaly-3"},
					},
					error:     ErrNoPrimary,
					primary:   noPrimary(),
					matchLogs: anyChange(primaryChanges{"virtual-storage-1": {"gitaly-1": {"demoted": 1, "promoted": 0}}}),
				},
			},
		},
		{
			desc: "doesnt elect replicas with delete_replica in ready state",
			state: state{
				"virtual-storage-1": {
					"relative-path-1": {
						"gitaly-1": {generation: 0, assigned: true},
					},
				},
			},
			existingJobs: []datastore.ReplicationEvent{
				{
					State: datastore.JobStateReady,
					Job: datastore.ReplicationJob{
						Change:            datastore.DeleteReplica,
						VirtualStorage:    "virtual-storage-1",
						RelativePath:      "relative-path-1",
						TargetNodeStorage: "gitaly-1",
					},
				},
			},
			steps: steps{
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-1"},
					},
					error:     ErrNoPrimary,
					primary:   noPrimary(),
					matchLogs: noChanges,
				},
			},
		},
		{
			desc: "doesnt elect replicas with delete_replica in in_progress state",
			state: state{
				"virtual-storage-1": {
					"relative-path-1": {
						"gitaly-1": {generation: 0, assigned: true},
					},
				},
			},
			existingJobs: []datastore.ReplicationEvent{
				{
					State: datastore.JobStateInProgress,
					Job: datastore.ReplicationJob{
						Change:            datastore.DeleteReplica,
						VirtualStorage:    "virtual-storage-1",
						RelativePath:      "relative-path-1",
						TargetNodeStorage: "gitaly-1",
					},
				},
			},
			steps: steps{
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-1"},
					},
					error:     ErrNoPrimary,
					primary:   noPrimary(),
					matchLogs: noChanges,
				},
			},
		},
		{
			desc: "doesnt elect replicas with delete_replica in failed state",
			state: state{
				"virtual-storage-1": {
					"relative-path-1": {
						"gitaly-1": {generation: 0, assigned: true},
					},
				},
			},
			existingJobs: []datastore.ReplicationEvent{
				{
					State: datastore.JobStateFailed,
					Job: datastore.ReplicationJob{
						Change:            datastore.DeleteReplica,
						VirtualStorage:    "virtual-storage-1",
						RelativePath:      "relative-path-1",
						TargetNodeStorage: "gitaly-1",
					},
				},
			},
			steps: steps{
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-1"},
					},
					error:     ErrNoPrimary,
					primary:   noPrimary(),
					matchLogs: noChanges,
				},
			},
		},
		{
			desc: "irrelevant delete_replica jobs are ignored",
			state: state{
				"virtual-storage-1": {
					"relative-path-1": {
						"gitaly-1": {generation: 0, assigned: true},
					},
				},
			},
			existingJobs: []datastore.ReplicationEvent{
				{
					State: datastore.JobStateReady,
					Job: datastore.ReplicationJob{
						Change:            datastore.DeleteReplica,
						VirtualStorage:    "wrong-virtual-storage",
						RelativePath:      "relative-path-1",
						TargetNodeStorage: "gitaly-1",
					},
				},
				{
					State: datastore.JobStateReady,
					Job: datastore.ReplicationJob{
						Change:            datastore.DeleteReplica,
						VirtualStorage:    "virtual-storage-1",
						RelativePath:      "wrong-relative-path",
						TargetNodeStorage: "gitaly-1",
					},
				},
				{
					State: datastore.JobStateReady,
					Job: datastore.ReplicationJob{
						Change:            datastore.DeleteReplica,
						VirtualStorage:    "virtual-storage-1",
						RelativePath:      "relative-path-1",
						TargetNodeStorage: "wrong-storage",
					},
				},
				{
					State: datastore.JobStateCancelled,
					Job: datastore.ReplicationJob{
						Change:            datastore.DeleteReplica,
						VirtualStorage:    "virtual-storage-1",
						RelativePath:      "relative-path-1",
						TargetNodeStorage: "gitaly-1",
					},
				},
				{
					State: datastore.JobStateDead,
					Job: datastore.ReplicationJob{
						Change:            datastore.DeleteReplica,
						VirtualStorage:    "virtual-storage-1",
						RelativePath:      "relative-path-1",
						TargetNodeStorage: "gitaly-1",
					},
				},
				{
					State: datastore.JobStateCompleted,
					Job: datastore.ReplicationJob{
						Change:            datastore.DeleteReplica,
						VirtualStorage:    "virtual-storage-1",
						RelativePath:      "relative-path-1",
						TargetNodeStorage: "gitaly-1",
					},
				},
			},
			steps: steps{
				{
					healthyNodes: map[string][]string{
						"virtual-storage-1": {"gitaly-1"},
					},
					primary:   any("gitaly-1"),
					matchLogs: anyChange(primaryChanges{"virtual-storage-1": {"gitaly-1": {"demoted": 0, "promoted": 1}}}),
				},
			},
		},
		{
			desc: "repository does not exist",
			steps: steps{
				{
					error:     commonerr.NewRepositoryNotFoundError("virtual-storage-1", "relative-path-1"),
					primary:   noPrimary(),
					matchLogs: noChanges,
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			db := getDB(t)

			rs := datastore.NewPostgresRepositoryStore(db, nil)
			for virtualStorage, relativePaths := range tc.state {
				for relativePath, storages := range relativePaths {
					_, err := db.ExecContext(ctx,
						`INSERT INTO repositories (virtual_storage, relative_path) VALUES ($1, $2)`,
						virtualStorage, relativePath,
					)
					require.NoError(t, err)

					for storage, record := range storages {
						require.NoError(t, rs.SetGeneration(ctx, virtualStorage, relativePath, storage, record.generation))

						if record.assigned {
							_, err := db.ExecContext(ctx, `
								INSERT INTO repository_assignments VALUES ($1, $2, $3)
							`, virtualStorage, relativePath, storage)
							require.NoError(t, err)
						}
					}
				}
			}

			for _, event := range tc.existingJobs {
				_, err := db.ExecContext(ctx,
					"INSERT INTO replication_queue (state, job) VALUES ($1, $2)",
					event.State, event.Job,
				)
				require.NoError(t, err)
			}

			for _, step := range tc.steps {
				runElection := func(tx *sql.Tx, matchLogs logMatcher) {
					setHealthyNodes(t, ctx, tx, map[string]map[string][]string{"praefect-0": step.healthyNodes})

					// The first transaction runs first
					logger, hook := test.NewNullLogger()
					elector := NewPerRepositoryElector(logrus.NewEntry(logger), tx)
					elector.handleError = func(err error) error { return err }

					trigger := make(chan struct{}, 1)
					trigger <- struct{}{}
					close(trigger)

					require.NoError(t, elector.Run(ctx, trigger))

					primary, err := elector.GetPrimary(ctx, "virtual-storage-1", "relative-path-1")
					assert.Equal(t, step.error, err)
					step.primary(t, primary)

					require.Len(t, hook.Entries, 3)
					matchLogs(t, hook.Entries[1])
				}

				// Run every step with two concurrent transactions to ensure two Praefect's running
				// election at the same time do not elect the primary multiple times. We begin both
				// transactions at the same time to ensure they have the same snapshot of the
				// database. The second transaction would be blocked until the first transaction commits.
				// To verify concurrent election runs do not elect the primary multiple times, we assert
				// the second transaction performed no changes and the primary is what the first run elected
				// it to be.
				txFirst, err := db.Begin()
				require.NoError(t, err)
				defer txFirst.Rollback()

				txSecond, err := db.Begin()
				require.NoError(t, err)
				defer txSecond.Rollback()

				runElection(txFirst, step.matchLogs)

				require.NoError(t, txFirst.Commit())

				// Run the second election on the same database snapshot. This should result in no changes.
				// Running this prior to the first transaction committing would block.
				runElection(txSecond, noChanges)

				require.NoError(t, txSecond.Commit())
			}
		})
	}
}

func TestPerRepositoryElector_Retry(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	dbCalls := 0
	handleErrorCalls := 0
	elector := NewPerRepositoryElector(
		testhelper.DiscardTestLogger(t),
		&glsql.MockQuerier{
			QueryContextFunc: func(context.Context, string, ...interface{}) (*sql.Rows, error) {
				dbCalls++

				return nil, assert.AnError
			},
		},
	)
	elector.retryWait = time.Nanosecond
	elector.handleError = func(err error) error {
		handleErrorCalls++
		require.True(t, errors.Is(err, assert.AnError))

		if handleErrorCalls == 2 {
			return context.Canceled
		}

		return nil
	}

	// we are only sending one trigger, second attempt must come from the retry logic
	trigger := make(chan struct{}, 1)
	trigger <- struct{}{}

	require.Equal(t, context.Canceled, elector.Run(ctx, trigger))
	require.Equal(t, 2, dbCalls)
	require.Equal(t, 2, handleErrorCalls)
}
