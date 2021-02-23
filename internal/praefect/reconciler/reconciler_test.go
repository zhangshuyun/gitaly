// +build postgres

package reconciler

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/metadatahandler"
	"gitlab.com/gitlab-org/gitaly/internal/praefect"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestMain(m *testing.M) {
	code := m.Run()

	if err := glsql.Clean(); err != nil {
		log.Fatalf("failed closing testing database: %v", err)
	}

	os.Exit(code)
}

func getDB(tb testing.TB) glsql.DB {
	return glsql.GetDB(tb, "reconciler")
}

func getStorageMethod(storage string) func() string {
	return func() string { return storage }
}

func TestReconciler(t *testing.T) {
	// repositories describes storage state as
	// virtual storage -> relative path -> physical storage -> generation

	type storageRecord struct {
		generation int
		assigned   bool
	}

	type repositories map[string]map[string]map[string]storageRecord
	type existingJobs []datastore.ReplicationEvent
	type jobs []datastore.ReplicationJob
	type storages map[string][]string

	configuredStorages := storages{
		"virtual-storage-1": {"storage-1", "storage-2", "storage-3"},
		// virtual storage 2 is here to ensure operations are correctly
		// scoped to a virtual storage
		"virtual-storage-2": {"storage-1", "storage-2", "storage-3"},
	}

	// configuredStoragesWithout returns a copy of the configureStorages
	// with the passed in storages removed.
	configuredStoragesWithout := func(omitStorage ...string) storages {
		out := storages{}
		for vs, storages := range configuredStorages {
			for _, storage := range storages {
				omitted := false
				for _, omit := range omitStorage {
					if storage == omit {
						omitted = true
						break
					}
				}

				if omitted {
					continue
				}

				out[vs] = append(out[vs], storage)
			}
		}
		return out
	}

	// generate existing jobs does a cartesian product between job states and change types and generates replication job
	// for each pair using the template job.
	generateExistingJobs := func(states []datastore.JobState, changeTypes []datastore.ChangeType, template datastore.ReplicationJob) existingJobs {
		var out existingJobs
		for _, state := range states {
			for _, changeType := range changeTypes {
				job := template
				job.Change = changeType
				out = append(out, datastore.ReplicationEvent{State: state, Job: job})
			}
		}

		return out
	}

	for _, tc := range []struct {
		desc               string
		healthyStorages    storages
		repositories       repositories
		existingJobs       existingJobs
		reconciliationJobs jobs
	}{
		{
			desc:               "no repositories",
			healthyStorages:    configuredStorages,
			reconciliationJobs: jobs{},
		},
		{
			desc:            "all up to date",
			healthyStorages: configuredStorages,
			repositories: repositories{
				"virtual-storage-1": {
					"relative-path-1": {
						"storage-1": {generation: 0},
						"storage-2": {generation: 0},
						"storage-3": {generation: 0},
					},
				},
			},
			reconciliationJobs: jobs{},
		},
		{
			desc:            "outdated repositories are reconciled",
			healthyStorages: configuredStorages,
			repositories: repositories{
				"virtual-storage-1": {
					"relative-path-1": {
						"storage-1": {generation: 1},
						"storage-2": {generation: 0},
					},
					"relative-path-2": {
						"storage-1": {generation: 0},
						"storage-2": {generation: 0},
						"storage-3": {generation: 0},
					},
				},
			},
			reconciliationJobs: jobs{
				{
					Change:            datastore.UpdateRepo,
					VirtualStorage:    "virtual-storage-1",
					RelativePath:      "relative-path-1",
					SourceNodeStorage: "storage-1",
					TargetNodeStorage: "storage-2",
				},
				{
					Change:            datastore.UpdateRepo,
					VirtualStorage:    "virtual-storage-1",
					RelativePath:      "relative-path-1",
					SourceNodeStorage: "storage-1",
					TargetNodeStorage: "storage-3",
				},
			},
		},
		{
			// generate number of jobs that exceeds the logBatchSize
			desc:            "reconciliation works with log batch size exceeded",
			healthyStorages: configuredStoragesWithout("storage-3"),
			repositories: func() repositories {
				repos := repositories{"virtual-storage-1": make(map[string]map[string]storageRecord, 2*logBatchSize+1)}
				for i := 0; i < 2*logBatchSize+1; i++ {
					repos["virtual-storage-1"][fmt.Sprintf("relative-path-%d", i)] = map[string]storageRecord{
						"storage-1": {generation: 1},
						"storage-2": {generation: 0},
					}
				}

				return repos
			}(),
			reconciliationJobs: func() jobs {
				var generated jobs
				for i := 0; i < 2*logBatchSize+1; i++ {
					generated = append(generated, datastore.ReplicationJob{
						Change:            datastore.UpdateRepo,
						VirtualStorage:    "virtual-storage-1",
						RelativePath:      fmt.Sprintf("relative-path-%d", i),
						SourceNodeStorage: "storage-1",
						TargetNodeStorage: "storage-2",
					})
				}

				return generated
			}(),
		},
		{
			desc:            "no healthy source to reconcile from",
			healthyStorages: configuredStoragesWithout("storage-1"),
			repositories: repositories{
				"virtual-storage-1": {
					"relative-path-1": {
						"storage-1": {generation: 1},
						"storage-2": {generation: 0},
					},
					"relative-path-2": {
						"storage-1": {generation: 1},
						"storage-2": {generation: 1},
						"storage-3": {generation: 1},
					},
				},
			},
			reconciliationJobs: jobs{},
		},
		{
			desc:            "unhealthy storage with outdated record is not reconciled",
			healthyStorages: configuredStoragesWithout("storage-2"),
			repositories: repositories{
				"virtual-storage-1": {
					"relative-path-1": {
						"storage-1": {generation: 1},
						"storage-2": {generation: 0},
					},
				},
			},
			reconciliationJobs: jobs{
				{
					Change:            datastore.UpdateRepo,
					VirtualStorage:    "virtual-storage-1",
					RelativePath:      "relative-path-1",
					SourceNodeStorage: "storage-1",
					TargetNodeStorage: "storage-3",
				},
			},
		},
		{
			desc:            "unhealthy storage with no record is not reconciled",
			healthyStorages: configuredStoragesWithout("storage-3"),
			repositories: repositories{
				"virtual-storage-1": {
					"relative-path-1": {
						"storage-1": {generation: 1},
						"storage-2": {generation: 0},
					},
				},
			},
			reconciliationJobs: jobs{
				{
					Change:            datastore.UpdateRepo,
					VirtualStorage:    "virtual-storage-1",
					RelativePath:      "relative-path-1",
					SourceNodeStorage: "storage-1",
					TargetNodeStorage: "storage-2",
				},
			},
		},
		{
			desc:            "repository with pending update is not reconciled",
			healthyStorages: configuredStorages,
			repositories: repositories{
				"virtual-storage-1": {
					"relative-path-1": {
						"storage-1": {generation: 1},
						"storage-2": {generation: 0},
					},
				},
			},
			existingJobs: existingJobs{{
				State: datastore.JobStateReady,
				Job: datastore.ReplicationJob{
					Change:            datastore.UpdateRepo,
					VirtualStorage:    "virtual-storage-1",
					RelativePath:      "relative-path-1",
					SourceNodeStorage: "storage-1",
					TargetNodeStorage: "storage-3",
				}},
			},
			reconciliationJobs: jobs{{
				Change:            datastore.UpdateRepo,
				VirtualStorage:    "virtual-storage-1",
				RelativePath:      "relative-path-1",
				SourceNodeStorage: "storage-1",
				TargetNodeStorage: "storage-2",
			}},
		},
		{
			desc:            "repository with only completed update jobs is reconciled",
			healthyStorages: configuredStoragesWithout("storage-3"),
			repositories: repositories{
				"virtual-storage-1": {
					"relative-path-1": {
						"storage-1": {generation: 1},
						"storage-2": {generation: 0},
					},
				},
			},
			existingJobs: generateExistingJobs(
				[]datastore.JobState{
					datastore.JobStateDead,
					datastore.JobStateCompleted,
					datastore.JobStateCancelled,
				},
				[]datastore.ChangeType{datastore.UpdateRepo},
				datastore.ReplicationJob{
					VirtualStorage:    "virtual-storage-1",
					RelativePath:      "relative-path-1",
					SourceNodeStorage: "storage-1",
					TargetNodeStorage: "storage-2",
				},
			),
			reconciliationJobs: jobs{{
				Change:            datastore.UpdateRepo,
				VirtualStorage:    "virtual-storage-1",
				RelativePath:      "relative-path-1",
				SourceNodeStorage: "storage-1",
				TargetNodeStorage: "storage-2",
			}},
		},
		{
			desc:            "repository with pending non-update jobs is reconciled",
			healthyStorages: configuredStoragesWithout("storage-2"),
			repositories: repositories{
				"virtual-storage-1": {
					"relative-path-1": {
						"storage-1": {generation: 1},
						"storage-2": {generation: 1},
					},
				},
			},
			existingJobs: generateExistingJobs(
				[]datastore.JobState{
					datastore.JobStateCancelled,
					datastore.JobStateCompleted,
					datastore.JobStateDead,
					datastore.JobStateReady,
					datastore.JobStateInProgress,
				},
				[]datastore.ChangeType{
					datastore.DeleteRepo,
					datastore.RenameRepo,
					datastore.GarbageCollect,
					datastore.RepackFull,
					datastore.RepackIncremental,
				},
				datastore.ReplicationJob{
					VirtualStorage:    "virtual-storage-1",
					RelativePath:      "relative-path-1",
					SourceNodeStorage: "storage-1",
					TargetNodeStorage: "storage-3",
				},
			),
			reconciliationJobs: jobs{{
				Change:            datastore.UpdateRepo,
				VirtualStorage:    "virtual-storage-1",
				RelativePath:      "relative-path-1",
				SourceNodeStorage: "storage-1",
				TargetNodeStorage: "storage-3",
			}},
		},
		{
			desc:            "unassigned node allowed to target an assigned node",
			healthyStorages: configuredStorages,
			repositories: repositories{
				"virtual-storage-1": {
					"relative-path-1": {
						"storage-1": {generation: 1},
						"storage-2": {generation: -1, assigned: true},
						"storage-3": {generation: 0, assigned: true},
					},
					// assert query correctly scopes for relative path
					"relative-path-2": {
						"storage-1": {generation: 2, assigned: true},
						"storage-2": {generation: 2, assigned: true},
						"storage-3": {generation: 2, assigned: true},
					},
				},
				// assert query correctly scopes for virtual storage
				"virtual-storage-2": {
					"relative-path-1": {
						"storage-1": {generation: 2, assigned: true},
						"storage-2": {generation: 2, assigned: true},
						"storage-3": {generation: 2, assigned: true},
					},
				},
			},
			reconciliationJobs: jobs{
				{
					Change:            datastore.UpdateRepo,
					VirtualStorage:    "virtual-storage-1",
					RelativePath:      "relative-path-1",
					SourceNodeStorage: "storage-1",
					TargetNodeStorage: "storage-2",
				},
				{
					Change:            datastore.UpdateRepo,
					VirtualStorage:    "virtual-storage-1",
					RelativePath:      "relative-path-1",
					SourceNodeStorage: "storage-1",
					TargetNodeStorage: "storage-3",
				},
			},
		},
		{
			desc:            "assigned node allowed to target an assigned node",
			healthyStorages: configuredStorages,
			repositories: repositories{
				"virtual-storage-1": {
					"relative-path-1": {
						"storage-1": {generation: 1, assigned: true},
						"storage-2": {generation: -1, assigned: true},
						"storage-3": {generation: 0, assigned: true},
					},
				},
			},
			reconciliationJobs: jobs{
				{
					Change:            datastore.UpdateRepo,
					VirtualStorage:    "virtual-storage-1",
					RelativePath:      "relative-path-1",
					SourceNodeStorage: "storage-1",
					TargetNodeStorage: "storage-2",
				},
				{
					Change:            datastore.UpdateRepo,
					VirtualStorage:    "virtual-storage-1",
					RelativePath:      "relative-path-1",
					SourceNodeStorage: "storage-1",
					TargetNodeStorage: "storage-3",
				},
			},
		},
		{
			desc:            "unassigned nodes are not targeted",
			healthyStorages: configuredStorages,
			repositories: repositories{
				"virtual-storage-1": {
					"relative-path-1": {
						"storage-1": {generation: 2, assigned: true},
						"storage-2": {generation: -1, assigned: false},
						"storage-3": {generation: 0, assigned: false},
					},
				},
			},
		},
		{
			desc:            "the only assigned node being up to date produces no jobs",
			healthyStorages: configuredStorages,
			repositories: repositories{
				"virtual-storage-1": {
					"relative-path-1": {
						"storage-1": {generation: 0, assigned: true},
					},
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			db := getDB(t)

			// set up the repository generation records expected by the test case
			rs := datastore.NewPostgresRepositoryStore(db, configuredStorages)
			for virtualStorage, relativePaths := range tc.repositories {
				for relativePath, storages := range relativePaths {
					for storage, repo := range storages {
						if repo.generation >= 0 {
							require.NoError(t, rs.SetGeneration(ctx, virtualStorage, relativePath, storage, repo.generation))
						}
					}

					for storage, repo := range storages {
						if repo.assigned {
							_, err := db.ExecContext(ctx, `
							INSERT INTO repository_assignments VALUES ($1, $2, $3)
						`, virtualStorage, relativePath, storage)
							require.NoError(t, err)
						}
					}
				}
			}

			// create the existing replication jobs the test expects
			var existingJobIDs []int64
			for _, existing := range tc.existingJobs {
				var id int64
				require.NoError(t, db.QueryRowContext(ctx, `
					INSERT INTO replication_queue (state, job)
					VALUES ($1, $2)
					RETURNING id
				`, existing.State, existing.Job).Scan(&id))
				existingJobIDs = append(existingJobIDs, id)
			}

			runReconcile := func(tx *sql.Tx) {
				t.Helper()

				runCtx, cancelRun := context.WithCancel(ctx)
				var stopped, resetted bool
				ticker := helper.NewManualTicker()
				ticker.StopFunc = func() { stopped = true }
				ticker.ResetFunc = func() {
					if resetted {
						cancelRun()
						return
					}

					resetted = true
					ticker.Tick()
				}

				reconciler := NewReconciler(
					testhelper.DiscardTestLogger(t),
					tx,
					praefect.StaticHealthChecker(tc.healthyStorages),
					configuredStorages,
					prometheus.DefBuckets,
				)
				reconciler.handleError = func(err error) error { return err }

				require.Equal(t, context.Canceled, reconciler.Run(runCtx, ticker))
				require.True(t, stopped)
				require.True(t, resetted)
			}

			// run reconcile in two concurrent transactions to ensure everything works
			// as expected with multiple Praefect's reconciling at the same time
			firstTx, err := db.Begin()
			require.NoError(t, err)
			defer firstTx.Rollback()

			secondTx, err := db.Begin()
			require.NoError(t, err)
			defer secondTx.Rollback()

			// the first reconcile acquires the reconciliation lock
			runReconcile(firstTx)

			// Concurrently reconcile from another transaction.
			// secondTx should not block as it won't attempt any insertions
			// as it failed to acquire the reconciliation lock.
			runReconcile(secondTx)
			require.NoError(t, secondTx.Commit())

			// commit the transaction of the first reconciliation
			require.NoError(t, firstTx.Commit())

			rows, err := db.QueryContext(ctx, `
				SELECT job, meta
				FROM replication_queue
				WHERE id NOT IN ( SELECT unnest($1::bigint[]) )
				`, pq.Int64Array(existingJobIDs),
			)
			require.NoError(t, err)
			defer rows.Close()

			actualJobs := make(jobs, 0, len(tc.reconciliationJobs))
			for rows.Next() {
				var job datastore.ReplicationJob
				var meta datastore.Params
				require.NoError(t, rows.Scan(&job, &meta))
				require.NotEmpty(t, meta[metadatahandler.CorrelationIDKey])
				actualJobs = append(actualJobs, job)
			}

			require.NoError(t, rows.Err())
			require.ElementsMatch(t, tc.reconciliationJobs, actualJobs)
		})
	}
}
