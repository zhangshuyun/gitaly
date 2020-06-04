package datastore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/lib/pq"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore/glsql"
)

// ReplicationEventQueue allows to put new events to the persistent queue and retrieve them back.
type ReplicationEventQueue interface {
	// Enqueue puts provided event into the persistent queue.
	Enqueue(ctx context.Context, event ReplicationEvent) (ReplicationEvent, error)
	// Dequeue retrieves events from the persistent queue using provided limitations and filters.
	Dequeue(ctx context.Context, virtualStorage, nodeStorage string, count int) ([]ReplicationEvent, error)
	// Acknowledge updates previously dequeued events with new state releasing resources acquired for it.
	// It only updates events that are in 'in_progress' state.
	// It returns list of ids that was actually acknowledged.
	Acknowledge(ctx context.Context, state JobState, ids []uint64) ([]uint64, error)
	// CountDeadReplicationJobs returns the dead replication job counts by relative path within the
	// given timerange. The timerange beginning is inclusive and ending is exclusive.
	CountDeadReplicationJobs(ctx context.Context, from, to time.Time) (map[string]int64, error)
	// GetOutdatedRepositories returns repositories with storages which have uncompleted replication jobs from the
	// reference storage.
	GetOutdatedRepositories(ctx context.Context, virtualStorage string, referenceStorage string) (map[string][]string, error)
	// GetUpToDateStorages returns list of target storages where latest replication job is in 'completed' state.
	// It returns no results if there is no up to date storages or there were no replication events yet.
	GetUpToDateStorages(ctx context.Context, virtualStorage, repoPath string) ([]string, error)
}

func allowToAck(state JobState) error {
	switch state {
	case JobStateCompleted, JobStateFailed, JobStateCancelled, JobStateDead:
		return nil
	default:
		return fmt.Errorf("event state is not supported: %q", state)
	}
}

// ReplicationJob is a persistent representation of the replication job.
type ReplicationJob struct {
	Change            ChangeType `json:"change"`
	RelativePath      string     `json:"relative_path"`
	TargetNodeStorage string     `json:"target_node_storage"`
	SourceNodeStorage string     `json:"source_node_storage"`
	VirtualStorage    string     `json:"virtual_storage"`
	Params            Params     `json:"params"`
}

func (job *ReplicationJob) Scan(value interface{}) error {
	if value == nil {
		return nil
	}

	d, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("unexpected type received: %T", value)
	}

	return json.Unmarshal(d, job)
}

func (job ReplicationJob) Value() (driver.Value, error) {
	data, err := json.Marshal(job)
	if err != nil {
		return nil, err
	}
	return string(data), nil
}

// ReplicationEvent is a persistent representation of the replication event.
type ReplicationEvent struct {
	ID        uint64
	State     JobState
	Attempt   int
	LockID    string
	CreatedAt time.Time
	UpdatedAt *time.Time
	Job       ReplicationJob
	Meta      Params
}

// Mapping returns list of references to the struct fields that correspond to the SQL columns/column aliases.
func (event *ReplicationEvent) Mapping(columns []string) ([]interface{}, error) {
	var mapping []interface{}
	for _, column := range columns {
		switch column {
		case "id":
			mapping = append(mapping, &event.ID)
		case "state":
			mapping = append(mapping, &event.State)
		case "created_at":
			mapping = append(mapping, &event.CreatedAt)
		case "updated_at":
			mapping = append(mapping, &event.UpdatedAt)
		case "attempt":
			mapping = append(mapping, &event.Attempt)
		case "lock_id":
			mapping = append(mapping, &event.LockID)
		case "job":
			mapping = append(mapping, &event.Job)
		case "meta":
			mapping = append(mapping, &event.Meta)
		default:
			return nil, fmt.Errorf("unknown column specified in SELECT statement: %q", column)
		}
	}
	return mapping, nil
}

// Scan fills receive fields with values fetched from database based on the set of columns/column aliases.
func (event *ReplicationEvent) Scan(columns []string, rows *sql.Rows) error {
	mappings, err := event.Mapping(columns)
	if err != nil {
		return err
	}
	return rows.Scan(mappings...)
}

// scanReplicationEvents reads all rows and convert them into structs filling all the fields according to fetched columns/column aliases.
func scanReplicationEvents(rows *sql.Rows) (events []ReplicationEvent, err error) {
	columns, err := rows.Columns()
	if err != nil {
		return events, err
	}

	defer func() {
		if cErr := rows.Close(); cErr != nil && err == nil {
			err = cErr
		}
	}()

	for rows.Next() {
		var event ReplicationEvent
		if err = event.Scan(columns, rows); err != nil {
			return events, err
		}
		events = append(events, event)
	}

	return events, rows.Err()
}

// interface implementation protection
var _ ReplicationEventQueue = PostgresReplicationEventQueue{}

// NewPostgresReplicationEventQueue returns new instance with provided Querier as a reference to storage.
func NewPostgresReplicationEventQueue(qc glsql.Querier) PostgresReplicationEventQueue {
	return PostgresReplicationEventQueue{qc: qc}
}

// PostgresReplicationEventQueue is a Postgres implementation of persistent queue.
type PostgresReplicationEventQueue struct {
	qc glsql.Querier
}

// CountDeadReplicationJobs returns the dead replication job counts by relative path within the
// given timerange. The timerange beginning is inclusive and ending is exclusive.
func (rq PostgresReplicationEventQueue) CountDeadReplicationJobs(ctx context.Context, from, to time.Time) (map[string]int64, error) {
	const q = `
		SELECT job->>'relative_path', count(*)
		FROM replication_queue
		WHERE	state 		= 'dead'
			AND created_at >= $1
			AND created_at  < $2
		GROUP BY job->>'relative_path';
	`

	rows, err := rq.qc.QueryContext(ctx, q, from.UTC(), to.UTC())
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			ctxlogrus.Extract(ctx).WithError(err).Error("error closing database rows")
		}
	}()

	out := map[string]int64{}
	for rows.Next() {
		var relativePath string
		var count int64

		if err := rows.Scan(&relativePath, &count); err != nil {
			return nil, err
		}

		out[relativePath] = count
	}

	return out, rows.Err()
}

func (rq PostgresReplicationEventQueue) Enqueue(ctx context.Context, event ReplicationEvent) (ReplicationEvent, error) {
	query := `
		WITH insert_lock AS (
			INSERT INTO replication_queue_lock(id)
			VALUES ($1 || '|' || $2 || '|' || $3)
			ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id
			RETURNING id
		)
		INSERT INTO replication_queue(lock_id, job, meta)
		SELECT insert_lock.id, $4, $5
		FROM insert_lock
		RETURNING id, state, created_at, updated_at, lock_id, attempt, job, meta`
	// this will always return a single row result (because of lock uniqueness) or an error
	rows, err := rq.qc.QueryContext(ctx, query, event.Job.VirtualStorage, event.Job.TargetNodeStorage, event.Job.RelativePath, event.Job, event.Meta)
	if err != nil {
		return ReplicationEvent{}, fmt.Errorf("query: %w", err)
	}

	events, err := scanReplicationEvents(rows)
	if err != nil {
		return ReplicationEvent{}, fmt.Errorf("scan: %w", err)
	}

	return events[0], nil
}

func (rq PostgresReplicationEventQueue) Dequeue(ctx context.Context, virtualStorage, nodeStorage string, count int) ([]ReplicationEvent, error) {
	query := `
		WITH to_lock AS (
			SELECT id
			FROM replication_queue_lock AS repo_lock
			WHERE repo_lock.acquired = FALSE AND repo_lock.id IN (
				SELECT lock_id
				FROM replication_queue
				WHERE attempt > 0
					AND state IN ('ready', 'failed')
					AND job->>'virtual_storage' = $1
					AND job->>'target_node_storage' = $2
				ORDER BY created_at
				LIMIT $3 FOR UPDATE
			)
			FOR UPDATE SKIP LOCKED
		)
		, jobs AS (
			UPDATE replication_queue AS queue
			SET attempt = queue.attempt - 1
				, state = 'in_progress'
				, updated_at = NOW() AT TIME ZONE 'UTC'
			FROM to_lock
			WHERE queue.lock_id IN (SELECT id FROM to_lock)
				AND state NOT IN ('in_progress', 'cancelled', 'completed')
				AND queue.id IN (
					SELECT id
					FROM replication_queue
					WHERE attempt > 0
						AND state IN ('ready', 'failed')
						AND job->>'virtual_storage' = $1
						AND job->>'target_node_storage' = $2
					ORDER BY created_at
					LIMIT $3
				)
			RETURNING queue.id, queue.state, queue.created_at, queue.updated_at, queue.lock_id, queue.attempt, queue.job, queue.meta
		)
		, track_job_lock AS (
			INSERT INTO replication_queue_job_lock (job_id, lock_id, triggered_at)
			SELECT jobs.id, jobs.lock_id, NOW() AT TIME ZONE 'UTC' FROM jobs
			RETURNING lock_id
		)
		, do_lock AS (
			UPDATE replication_queue_lock
			SET acquired = TRUE
			WHERE id IN (SELECT lock_id FROM track_job_lock)
		)
		SELECT id, state, created_at, updated_at, lock_id, attempt, job, meta
		FROM jobs
		ORDER BY id`
	rows, err := rq.qc.QueryContext(ctx, query, virtualStorage, nodeStorage, count)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	res, err := scanReplicationEvents(rows)
	if err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}

	return res, nil
}

// Acknowledge updates previously dequeued events with new state releasing resources acquired for it.
// It only updates events that are in 'in_progress' state.
// It returns list of ids that was actually acknowledged.
func (rq PostgresReplicationEventQueue) Acknowledge(ctx context.Context, state JobState, ids []uint64) ([]uint64, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	if err := allowToAck(state); err != nil {
		return nil, err
	}

	params := glsql.NewParamsAssembler()
	query := `
		WITH existing AS (
			SELECT id, lock_id
			FROM replication_queue
			WHERE id IN (` + params.AddParams(glsql.Uint64sToInterfaces(ids...)) + `)
			AND state = 'in_progress'
			FOR UPDATE
		)
		, to_release AS (
			UPDATE replication_queue AS queue
			SET state = ` + params.AddParam(state) + `
			FROM existing
			WHERE existing.id = queue.id
			RETURNING queue.id, queue.lock_id
		)
		, removed_job_lock AS (
			DELETE FROM replication_queue_job_lock AS job_lock
			USING to_release AS job_failed
			WHERE job_lock.job_id = job_failed.id AND job_lock.lock_id = job_failed.lock_id
			RETURNING job_failed.lock_id
		)
		, release AS (
			UPDATE replication_queue_lock
			SET acquired = FALSE
			WHERE id IN (
				SELECT existing.lock_id
				FROM (SELECT lock_id, COUNT(*) AS amount FROM removed_job_lock GROUP BY lock_id) AS removed
				JOIN (
					SELECT lock_id, COUNT(*) AS amount
					FROM replication_queue_job_lock
					WHERE lock_id IN (SELECT lock_id FROM removed_job_lock)
					GROUP BY lock_id
				) AS existing ON removed.lock_id = existing.lock_id AND removed.amount = existing.amount
			)
		)
		SELECT id
		FROM existing`
	rows, err := rq.qc.QueryContext(ctx, query, params.Params()...)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	var acknowledged glsql.Uint64Provider
	if err := glsql.ScanAll(rows, &acknowledged); err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}

	return acknowledged.Values(), nil
}

// GetOutdatedRepositories gets repositories which have not replicated all writes from reference node.
func (rq PostgresReplicationEventQueue) GetOutdatedRepositories(ctx context.Context, virtualStorage, reference string) (map[string][]string, error) {
	// This query searches the replication graph for a path from a repository to the repository on the reference storage.
	// If it does not find a path to the reference storage, the repository's copy on the storage is reported as outdated.
	//
	// The path is constructed by following the chain of replication jobs. `latest_job` CTE queries for the latest
	// 'meaningful' jobs for each repository and storage. Job is considered meaningful in two cases:
	//   1. Jobs which originate from the reference are meaningful as if they are 'completed', they indicate the
	//      repository is up to date on the node. If they are in any other state, the job indicates the repository
	//      is missing some writes from the reference storage.
	//   2. Jobs which originate from other nodes than the reference are only considered meaningful if they are
	//      in 'completed' state. If the job is 'completed', then the repository contains the latest changes from
	//      the given non-reference node and we have to then follow the chain to verify the non-reference node is
	//      up to date with the reference. In other states, the node does not contain the non-reference node's
	//      changes and the job does not give any information on whether the currently checked node is missing
	//      writes from the reference.
	//
	// Taking the meaningful jobs as starting points, `intermediate_job` CTE then follows the replication job chain
	// until it finds the path to the reference storage or runs in to a cycle. The next job to follow in the chain
	// is decided using the same meaningful job criteria as in `latest_job` CTE.
	//
	// As `intermediate_job` returns every path segment, `full_path` CTE finally selects the full (longest) path for
	// each repository and node pair.
	//
	// Finally we select the repository/node pairs which do not have reach the reference repository or reach it but
	// did not successfully complete the replication job from it.
	const q = `
WITH RECURSIVE latest_job AS (
	SELECT DISTINCT ON (repository, target)
		job->>'relative_path' AS repository,
		job->>'target_node_storage' AS target,
		job->>'source_node_storage' AS source,
		state
	FROM replication_queue
	WHERE
		job->>'virtual_storage' = $1 AND
		(job->>'source_node_storage' = $2 OR
		job->>'source_node_storage' != $2 AND state = 'completed')
	ORDER BY repository, target, updated_at DESC NULLS FIRST
), intermediate_job AS (
	SELECT
		repository,
		target,
		state,
		ARRAY[source] AS path
	FROM latest_job
	UNION
		SELECT
			intermediate_job.repository,
			intermediate_job.target,
			next_job.state,
			intermediate_job.path || (job->>'source_node_storage')
		FROM replication_queue AS next_job, intermediate_job
		WHERE
			job->>'source_node_storage' != ALL(intermediate_job.path) AND
			job->>'virtual_storage' = $1 AND
			job->>'relative_path' = intermediate_job.repository AND
			job->>'target_node_storage' = intermediate_job.path[array_length(intermediate_job.path, 1)] AND
			(
				job->>'source_node_storage' = $2 OR
				job->>'source_node_storage' != $2 AND next_job.state = 'completed'
			)
), full_path AS (
	SELECT DISTINCT ON (repository, target)
		repository,
		target,
		state,
		path
	FROM intermediate_job
	ORDER BY repository, target, array_length(path, 1) DESC
)

SELECT repository, target, state, path
FROM full_path
WHERE state != 'completed' OR path[array_length(path, 1)] != $2
ORDER BY repository, target
`

	rows, err := rq.qc.QueryContext(ctx, q, virtualStorage, reference)
	if err != nil {
		return nil, err
	}

	repos := map[string][]string{}
	for rows.Next() {
		var repo, node, state string
		var path []string
		if err := rows.Scan(&repo, &node, &state, (*pq.StringArray)(&path)); err != nil {
			return nil, err
		}

		repos[repo] = append(repos[repo], node)
		log.Printf("repo: %s, node: %s, state: %s, path: %s", repo, node, state, path)
	}

	defer rows.Close()

	return repos, rows.Err()
}

func (rq PostgresReplicationEventQueue) GetUpToDateStorages(ctx context.Context, virtualStorage, repoPath string) ([]string, error) {
	query := `
		SELECT storage
		FROM (
			SELECT DISTINCT ON (job ->> 'target_node_storage')
				job ->> 'target_node_storage' AS storage,
				state
			FROM replication_queue
			WHERE job ->> 'virtual_storage' = $1 AND job ->> 'relative_path' = $2
			ORDER BY job ->> 'target_node_storage', updated_at DESC NULLS FIRST
		) t
		WHERE state = 'completed'`
	rows, err := rq.qc.QueryContext(ctx, query, virtualStorage, repoPath)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	var storages glsql.StringProvider
	if err := glsql.ScanAll(rows, &storages); err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}

	return storages.Values(), nil
}
