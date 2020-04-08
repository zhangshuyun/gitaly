package datastore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore/glsql"
)

// ReplicationEventQueue allows to put new events to the persistent queue and retrieve them back.
type ReplicationEventQueue interface {
	// Enqueue puts provided event into the persistent queue.
	Enqueue(ctx context.Context, event ReplicationEvent) (ReplicationEvent, error)
	// Dequeue retrieves events from the persistent queue using provided limitations and filters.
	Dequeue(ctx context.Context, nodeStorage string, count int) ([]ReplicationEvent, error)
	// Acknowledge updates previously dequeued events with new state releasing resources acquired for it.
	// It only updates events that are in 'in_progress' state.
	// It returns list of ids that was actually acknowledged.
	Acknowledge(ctx context.Context, state JobState, ids []uint64) ([]uint64, error)
	// CountDeadReplicationJobs returns the dead replication job counts by relative path within the
	// given timerange. The timerange beginning is inclusive and ending is exclusive.
	CountDeadReplicationJobs(ctx context.Context, from, to time.Time) (map[string]int64, error)
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

// ScanReplicationEvents reads all rows and convert them into structs filling all the fields according to fetched columns/column aliases.
func ScanReplicationEvents(rows *sql.Rows) (events []ReplicationEvent, err error) {
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
			VALUES ($1 || '|' || $2)
			ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id
			RETURNING id
		)
		INSERT INTO replication_queue(lock_id, job, meta)
		SELECT insert_lock.id, $3, $4
		FROM insert_lock
		RETURNING id, state, created_at, updated_at, lock_id, attempt, job, meta`
	// this will always return a single row result (because of lock uniqueness) or an error
	rows, err := rq.qc.QueryContext(ctx, query, event.Job.TargetNodeStorage, event.Job.RelativePath, event.Job, event.Meta)
	if err != nil {
		return ReplicationEvent{}, err
	}

	events, err := ScanReplicationEvents(rows)
	if err != nil {
		return ReplicationEvent{}, err
	}

	return events[0], nil
}

func (rq PostgresReplicationEventQueue) Dequeue(ctx context.Context, nodeStorage string, count int) ([]ReplicationEvent, error) {
	query := `
		WITH to_lock AS (
			SELECT id
			FROM replication_queue_lock AS repo_lock
			WHERE repo_lock.acquired = FALSE AND repo_lock.id IN (
				SELECT lock_id
				FROM replication_queue
				WHERE attempt > 0 AND state IN ('ready', 'failed') AND job->>'target_node_storage' = $1
				ORDER BY created_at
				LIMIT $2 FOR UPDATE
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
					WHERE attempt > 0 AND state IN ('ready', 'failed') AND job->>'target_node_storage' = $1
					ORDER BY created_at
					LIMIT $2
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
	rows, err := rq.qc.QueryContext(ctx, query, nodeStorage, count)
	if err != nil {
		return nil, err
	}

	return ScanReplicationEvents(rows)
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
		return nil, err
	}

	var acknowledged glsql.Uint64Provider
	if err := glsql.ScanAll(rows, &acknowledged); err != nil {
		return nil, err
	}

	return acknowledged.Values(), nil
}
