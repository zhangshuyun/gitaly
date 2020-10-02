// +build postgres

package reconciler

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v13/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v13/internal/middleware/metadatahandler"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
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
	type repositories map[string]map[string]map[string]int
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
						"storage-1": 0,
						"storage-2": 0,
						"storage-3": 0,
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
						"storage-1": 1,
						"storage-2": 0,
					},
					"relative-path-2": {
						"storage-1": 0,
						"storage-2": 0,
						"storage-3": 0,
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
			desc:            "no healthy source to reconcile from",
			healthyStorages: configuredStoragesWithout("storage-1"),
			repositories: repositories{
				"virtual-storage-1": {
					"relative-path-1": {
						"storage-1": 1,
						"storage-2": 0,
					},
					"relative-path-2": {
						"storage-1": 1,
						"storage-2": 1,
						"storage-3": 1,
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
						"storage-1": 1,
						"storage-2": 0,
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
						"storage-1": 1,
						"storage-2": 0,
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
						"storage-1": 1,
						"storage-2": 0,
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
						"storage-1": 1,
						"storage-2": 0,
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
						"storage-1": 1,
						"storage-2": 1,
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
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			db := getDB(t)

			// set up the repository generation records expected by the test case
			rs := datastore.NewPostgresRepositoryStore(db, configuredStorages)
			for virtualStorage, relativePaths := range tc.repositories {
				for relativePath, storages := range relativePaths {
					for storage, generation := range storages {
						require.NoError(t, rs.SetGeneration(ctx, virtualStorage, relativePath, storage, generation))
					}
				}
			}

			// create the existing replication jobs the test expects
			queue := datastore.NewPostgresReplicationEventQueue(db)
			existingJobs := make(map[uint64]bool, len(tc.existingJobs))
			for _, existing := range tc.existingJobs {
				event, err := queue.Enqueue(ctx, existing)
				require.NoError(t, err)
				existingJobs[event.ID] = true

				if existing.State == datastore.JobStateCompleted ||
					existing.State == datastore.JobStateDead ||
					existing.State == datastore.JobStateCancelled {
					// get the event in the correct state.
					event, err := queue.Dequeue(ctx, existing.Job.VirtualStorage, existing.Job.TargetNodeStorage, 1)
					require.NoError(t, err)
					require.Len(t, event, 1)

					acked, err := queue.Acknowledge(ctx, existing.State, []uint64{event[0].ID})
					require.NoError(t, err)
					require.Equal(t, []uint64{event[0].ID}, acked)
				}
			}

			nm := &nodes.MockManager{
				GetShardFunc: func(virtualStorage string) (nodes.Shard, error) {
					storages, ok := configuredStorages[virtualStorage]
					if !ok {
						t.Fatalf("unconfigured virtual storage: %s", virtualStorage)
					}

					shard := nodes.Shard{}
					for i, storage := range storages {
						isHealthy := false
						for _, healthy := range tc.healthyStorages[virtualStorage] {
							if healthy == storage {
								isHealthy = true
								break
							}
						}

						node := &nodes.MockNode{GetStorageMethod: getStorageMethod(storage), Healthy: isHealthy}
						if i == 0 {
							// First node is picked to be the primary just to fill the shard object.
							// Reconciliation logic does not differentiate between primaries and secondaries.
							shard.Primary = node
						} else {
							shard.Secondaries = append(shard.Secondaries, node)
						}
					}

					return shard, nil
				},
			}

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

			reconciler := NewReconciler(testhelper.DiscardTestLogger(t), db, nm, configuredStorages, prometheus.DefBuckets)
			reconciler.handleError = func(err error) error { return err }

			err := reconciler.Run(runCtx, ticker)
			require.Equal(t, context.Canceled, err)
			require.True(t, stopped)
			require.True(t, resetted)

			actualJobs := make(jobs, 0, len(tc.reconciliationJobs))
			for virtualStorage, storages := range configuredStorages {
				for _, storage := range storages {
					// dequeue all of the events in the queue
					events, err := queue.Dequeue(ctx, virtualStorage, storage, 99999999999)
					require.NoError(t, err)
					for _, event := range events {
						if existingJobs[event.ID] {
							// filter out jobs the test expected to be in the queue already
							continue
						}

						require.NotEmpty(t, event.Meta[metadatahandler.CorrelationIDKey])
						actualJobs = append(actualJobs, event.Job)
					}
				}
			}

			require.Equal(t, tc.reconciliationJobs, actualJobs)
		})
	}
}
