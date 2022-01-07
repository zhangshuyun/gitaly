package datastore

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testdb"
)

func TestRepositoryStoreCollector(t *testing.T) {
	t.Parallel()
	ctx, cancel := testhelper.Context()
	defer cancel()

	type replicas map[string]struct {
		generation int
		assigned   bool
	}

	type repositories []struct {
		deleted      bool
		relativePath string
		replicas     replicas
	}

	db := testdb.New(t)

	for _, tc := range []struct {
		desc         string
		healthyNodes []string
		repositories repositories
		timeout      bool
		count        int
		error        error
	}{
		{
			desc: "no repositories",
		},
		{
			desc: "deleted repositories are not considered unavailable",
			repositories: repositories{
				{
					deleted:      true,
					relativePath: "repository-1",
					replicas: replicas{
						"storage-1": {generation: 0},
					},
				},
			},
		},
		{
			desc: "repositories without any healthy replicas are counted",
			repositories: repositories{
				{
					relativePath: "repository-1",
					replicas: replicas{
						"storage-1": {generation: 0},
					},
				},
			},
			count: 1,
		},
		{
			desc:         "repositories with healthy replicas are not counted",
			healthyNodes: []string{"storage-1"},
			repositories: repositories{
				{
					relativePath: "repository-1",
					replicas: replicas{
						"storage-1": {generation: 0},
					},
				},
			},
		},
		{
			desc:         "repositories with only outdated healthy replicas are counted",
			healthyNodes: []string{"storage-1"},
			repositories: repositories{
				{
					relativePath: "repository-1",
					replicas: replicas{
						"storage-1": {generation: 0},
						"storage-2": {generation: 1},
					},
				},
			},
			count: 1,
		},
		{
			desc:         "repositories with unassigned fully up to date healthy replicas are not counted",
			healthyNodes: []string{"storage-2"},
			repositories: repositories{
				{
					relativePath: "repository-1",
					replicas: replicas{
						"storage-1": {generation: 1, assigned: true},
						"storage-2": {generation: 1},
					},
				},
			},
		},
		{
			desc:         "repositories with unassigned, outdated replicas is not unavailable",
			healthyNodes: []string{"storage-1"},
			repositories: repositories{
				{
					relativePath: "repository-1",
					replicas: replicas{
						"storage-1": {generation: 1, assigned: true},
						"storage-2": {generation: 0},
					},
				},
			},
		},
		{
			desc:         "multiple unavailable repositories are counted correctly",
			healthyNodes: []string{"storage-2"},
			repositories: repositories{
				{
					relativePath: "repository-1",
					replicas: replicas{
						"storage-1": {generation: 1},
						"storage-2": {generation: 0},
					},
				},
				{
					relativePath: "repository-2",
					replicas: replicas{
						"storage-1": {generation: 1},
						"storage-2": {generation: 0},
					},
				},
			},
			count: 2,
		},
		{
			desc:    "query timeout",
			timeout: true,
			error:   fmt.Errorf("query: %w", context.DeadlineExceeded),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			tx := db.Begin(t)
			defer tx.Rollback(t)

			testdb.SetHealthyNodes(t, ctx, tx, map[string]map[string][]string{
				"praefect-0": {"virtual-storage-1": tc.healthyNodes},
			})

			for _, repository := range tc.repositories {

				rs := NewPostgresRepositoryStore(tx, nil)

				var repositoryID int64
				for storage, replica := range repository.replicas {
					if repositoryID == 0 {
						const virtualStorage = "virtual-storage-1"

						var err error
						repositoryID, err = rs.ReserveRepositoryID(ctx, virtualStorage, repository.relativePath)
						require.NoError(t, err)

						require.NoError(t, rs.CreateRepository(ctx, repositoryID, virtualStorage, repository.relativePath, repository.relativePath, storage, nil, nil, false, false))
					}

					if replica.assigned {
						_, err := tx.ExecContext(ctx, `
							INSERT INTO repository_assignments (repository_id, virtual_storage, relative_path, storage)
							VALUES ($1, 'virtual-storage-1', $2, $3)
						`, repositoryID, repository.relativePath, storage,
						)
						require.NoError(t, err)
					}

					require.NoError(t, rs.SetGeneration(ctx, repositoryID, storage, repository.relativePath, replica.generation))
				}

				if repository.deleted {
					_, err := tx.ExecContext(ctx, `
						DELETE FROM repositories WHERE repository_id = $1
					`, repositoryID,
					)
					require.NoError(t, err)
				}
			}

			timeout := time.Hour
			if tc.timeout {
				timeout = 0
			}

			logger, hook := test.NewNullLogger()
			c := NewRepositoryStoreCollector(logger, []string{"virtual-storage-1", "virtual-storage-2"}, tx, timeout)
			err := testutil.CollectAndCompare(c, strings.NewReader(fmt.Sprintf(`
# HELP gitaly_praefect_read_only_repositories Number of repositories in read-only mode within a virtual storage.
# TYPE gitaly_praefect_read_only_repositories gauge
gitaly_praefect_read_only_repositories{virtual_storage="virtual-storage-1"} %d
gitaly_praefect_read_only_repositories{virtual_storage="virtual-storage-2"} 0
# HELP gitaly_praefect_unavailable_repositories Number of repositories that have no healthy, up to date replicas.
# TYPE gitaly_praefect_unavailable_repositories gauge
gitaly_praefect_unavailable_repositories{virtual_storage="virtual-storage-1"} %d
gitaly_praefect_unavailable_repositories{virtual_storage="virtual-storage-2"} 0
			`, tc.count, tc.count)))
			if tc.error != nil {
				require.Equal(t, "failed collecting read-only repository count metric", hook.Entries[0].Message)
				require.Equal(t, logrus.Fields{"error": tc.error, "component": "RepositoryStoreCollector"}, hook.Entries[0].Data)
				return
			}

			require.NoError(t, err)
		})
	}
}

type checkIfQueriedDB struct {
	queried bool
}

func (c *checkIfQueriedDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	c.queried = true
	return nil, errors.New("QueryContext should not be called")
}

func (c *checkIfQueriedDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	c.queried = true
	return &sql.Row{}
}

func (c *checkIfQueriedDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	c.queried = true
	return nil, errors.New("ExecContext should not be called")
}

func TestRepositoryStoreCollector_CollectNotCalledOnRegister(t *testing.T) {
	logger, _ := test.NewNullLogger()

	var db checkIfQueriedDB
	c := NewRepositoryStoreCollector(logger, []string{"virtual-storage-1", "virtual-storage-2"}, &db, 2*time.Second)
	registry := prometheus.NewRegistry()
	registry.MustRegister(c)

	assert.False(t, db.queried)
}

func TestRepositoryStoreCollector_ReplicationQueueDepth(t *testing.T) {
	db := testdb.New(t)
	ctx, cancel := testhelper.Context()
	defer cancel()

	log := testhelper.NewDiscardingLogger(t)

	storageNames := map[string][]string{
		"praefect-0": {"storage-0", "storage-1", "storage-2"},
		"praefect-1": {"storage-3", "storage-4", "storage-5"},
	}
	queue := NewPostgresReplicationEventQueue(db.DB)

	readyJobs := 5
	for virtualStorage, nodes := range storageNames {
		for i := 0; i < readyJobs; i++ {
			_, err := queue.Enqueue(ctx, ReplicationEvent{
				Job: ReplicationJob{
					Change:            UpdateRepo,
					RelativePath:      "/project/path-1",
					TargetNodeStorage: nodes[1],
					SourceNodeStorage: nodes[0],
					VirtualStorage:    virtualStorage,
					Params:            nil,
				},
			})
			require.NoError(t, err)
		}
	}

	collector := NewQueueDepthCollector(log, db, 1*time.Hour)

	require.NoError(t, testutil.CollectAndCompare(collector, bytes.NewBufferString(fmt.Sprintf(`
# HELP gitaly_praefect_replication_queue_depth Number of jobs in the replication queue
# TYPE gitaly_praefect_replication_queue_depth gauge
gitaly_praefect_replication_queue_depth{state="ready",target_node="storage-1",virtual_storage="praefect-0"} %d
gitaly_praefect_replication_queue_depth{state="ready",target_node="storage-4",virtual_storage="praefect-1"} %d
`, readyJobs, readyJobs))))

	var eventIDs []uint64
	events, err := queue.Dequeue(ctx, "praefect-0", "storage-1", 1)
	require.NoError(t, err)
	require.Len(t, events, 1)
	eventIDs = append(eventIDs, events[0].ID)

	events, err = queue.Dequeue(ctx, "praefect-1", "storage-4", 1)
	require.NoError(t, err)
	require.Len(t, events, 1)
	eventIDs = append(eventIDs, events[0].ID)

	require.NoError(t, testutil.CollectAndCompare(collector, bytes.NewBufferString(fmt.Sprintf(`
# HELP gitaly_praefect_replication_queue_depth Number of jobs in the replication queue
# TYPE gitaly_praefect_replication_queue_depth gauge
gitaly_praefect_replication_queue_depth{state="in_progress",target_node="storage-1",virtual_storage="praefect-0"} %d
gitaly_praefect_replication_queue_depth{state="in_progress",target_node="storage-4",virtual_storage="praefect-1"} %d
gitaly_praefect_replication_queue_depth{state="ready",target_node="storage-1",virtual_storage="praefect-0"} %d
gitaly_praefect_replication_queue_depth{state="ready",target_node="storage-4",virtual_storage="praefect-1"} %d
`, 1, 1, readyJobs-1, readyJobs-1))))

	_, err = queue.Acknowledge(ctx, JobStateFailed, eventIDs)
	require.NoError(t, err)

	require.NoError(t, testutil.CollectAndCompare(collector, bytes.NewBufferString(fmt.Sprintf(`
# HELP gitaly_praefect_replication_queue_depth Number of jobs in the replication queue
# TYPE gitaly_praefect_replication_queue_depth gauge
gitaly_praefect_replication_queue_depth{state="failed",target_node="storage-1",virtual_storage="praefect-0"} %d
gitaly_praefect_replication_queue_depth{state="failed",target_node="storage-4",virtual_storage="praefect-1"} %d
gitaly_praefect_replication_queue_depth{state="ready",target_node="storage-1",virtual_storage="praefect-0"} %d
gitaly_praefect_replication_queue_depth{state="ready",target_node="storage-4",virtual_storage="praefect-1"} %d
`, 1, 1, readyJobs-1, readyJobs-1))))
}
