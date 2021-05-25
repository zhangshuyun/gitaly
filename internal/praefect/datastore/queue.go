package datastore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
)

// ReplicationEventQueue allows to put new events to the persistent queue and retrieve them back.
type ReplicationEventQueue interface {
	// Enqueue puts provided event into the persistent queue.
	Enqueue(ctx context.Context, event ReplicationEvent) (ReplicationEvent, error)
	// Dequeue retrieves events from the persistent queue using provided limitations and filters.
	Dequeue(ctx context.Context, virtualStorage, nodeStorage string, count int) ([]ReplicationEvent, error)
	// Acknowledge updates previously dequeued events with the new state and releases resources acquired for it.
	// It updates events that are in 'in_progress' state to the state that is passed in.
	// It also updates state of similar events (scheduled fot the same repository with same change from the same source)
	// that are in 'ready' state and created before the target event was dequeue for the processing if the new state is
	// 'completed'. Otherwise it won't be changed.
	// It returns sub-set of passed in ids that were updated.
	Acknowledge(ctx context.Context, state JobState, ids []uint64) ([]uint64, error)
	// StartHealthUpdate starts periodical update of the event's health identifier.
	// The events with fresh health identifier won't be considered as stale.
	// The health update will be executed on each new entry received from trigger channel passed in.
	// It is a blocking call that is managed by the passed in context.
	StartHealthUpdate(ctx context.Context, trigger <-chan time.Time, events []ReplicationEvent) error
	// AcknowledgeStale moves replication events that are 'in_progress' state for too long (more than staleAfter)
	// into the next state:
	//   'failed' - in case it has more attempts to be executed
	//   'dead' - in case it has no more attempts to be executed
	AcknowledgeStale(ctx context.Context, staleAfter time.Duration) error
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
	// The main requirements for the queue implementation are:
	//  - it should not use long transactions
	//  - it should perform without problems if PgBouncer is used in between with `pool_mode = transaction`
	//  - it should perform concurrently with other queue implementations (support of horizontal scaling)
	//  - it should handle events sequentially starting with the oldest
	//  - it should handle events concurrently for multiple repositories
	//  - it should support retries
	//
	// Current implementation uses the following tables to mimic the queue:
	//  - replication_queue_lock
	//  - replication_queue
	//  - replication_queue_job_lock
	//
	// `replication_queue_lock` holds repository level locks to synchronize multiple Praefects instances
	// working on the same queue (shared database). Only one worker is allowed to operate on a given repository at a time.
	// The `id` column is a concatenated string of the virtual storage name, gitaly node,
	// and repository relative path: virtual1|node1|/path/to/project.git with `|` as a delimiter (represented as <lock>
	// elsewhere in this doc).
	// The `acquired` column reflects whether the lock for this repository (qualified by `id` column) is taken.
	//
	// `replication_queue` stores the actual replication event. The job is stored as a JSONB value in the `job` column of
	// the table. It includes `meta` column designed to store meta information such as `correlation_id` etc. Each event has
	// corresponding value in the `lock_id` column from `replication_queue_lock` table. Each replication event will be
	// created with the following defaults:
	//  - attempt: 3
	//  - state: `ready`
	//  - created_at: UTC timestamp
	//  - updated_at: NULL
	//
	// `replication_queue_job_lock` holds event specific locks to prevent multiple queue workers from operating on the same
	// event and track the events that are protected by the <lock>.
	//
	// The mechanics of how the queue works is described in the `Enqueue`, `Dequeue`, `Acknowledge` methods.
	qc glsql.Querier
}

func (rq PostgresReplicationEventQueue) Enqueue(ctx context.Context, event ReplicationEvent) (ReplicationEvent, error) {
	// When `Enqueue` method is called:
	//  1. Insertion of the new record into `replication_queue_lock` table, so we are ensured all events have
	//     a corresponding <lock>. If a record already exists it won't be inserted again.
	//  2. Insertion of the new record into the `replication_queue` table with the defaults listed above,
	//     the job, the meta and corresponding <lock> used in `replication_queue_lock` table for the `lock_id` column.

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
	// When `Dequeue` method is called:
	//  1. Events with attempts left that are either in `ready` or `failed` state are candidates for dequeuing.
	//     Events already being processed by another worker are filtered out by checking if the event is already locked
	//     in the `replication_queue_job_lock` table.
	//  2. Events for repositories that are already locked by another Praefect instance are filtered out.
	//     Repository locks are stored in the `replication_queue_lock` table.
	//  3. The events that still remain after filtering are dequeued. On dequeuing:
	//      - The event's attempts are decremented by 1.
	//      - The event's state is set to `in_progress`
	//      - The event's `updated_at` is set to current time in UTC.
	//  4. For each event retrieved from the step above a new record would be created in
	//     `replication_queue_job_lock` table. Rows in this table allows us to track events that were fetched for processing
	//     and relation of them with the locks in the `replication_queue_lock` table. The reason we need it is because
	//     multiple events can be fetched for the same repository (more details on it in `Acknowledge` below).
	//  5. Update the corresponding <lock> in `replication_queue_lock` table and column `acquired` is assigned with
	//     `TRUE` value to signal that this <lock> is busy and can't be used to fetch events (step 2.).
	//
	//  As a special case, 'delete_replica' type events have unlimited attempts. This is to ensure we never partially apply the job
	//  by deleting the repository from the disk but leaving it still present in the database. Praefect would then see that there still
	//  is a replica on the storage, when there is none in fact. That could cause us to delete all replicas of a repository.

	query := `
		WITH lock AS (
			SELECT id
			FROM replication_queue_lock
			WHERE id LIKE ($1 || '|' || $2 || '|%') AND NOT acquired
			FOR UPDATE SKIP LOCKED
		)
		, candidate AS (
			SELECT id
			FROM replication_queue
			WHERE id IN (
				SELECT DISTINCT FIRST_VALUE(queue.id) OVER (PARTITION BY lock_id, job->>'change'  ORDER BY queue.created_at)
				FROM replication_queue AS queue
				JOIN lock ON queue.lock_id = lock.id
				WHERE queue.state IN ('ready', 'failed' )
					AND NOT EXISTS (SELECT 1 FROM replication_queue_job_lock WHERE lock_id = queue.lock_id)
			)
			ORDER BY created_at
			LIMIT $3
			FOR UPDATE
		)
		, job AS (
			UPDATE replication_queue AS queue
			SET attempt = CASE WHEN job->>'change' = 'delete_replica' THEN queue.attempt ELSE queue.attempt - 1 END
				, state = 'in_progress'
				, updated_at = NOW() AT TIME ZONE 'UTC'
			FROM candidate
			WHERE queue.id = candidate.id
			RETURNING queue.id, queue.state, queue.created_at, queue.updated_at, queue.lock_id, queue.attempt, queue.job, queue.meta
		)
		, track_job_lock AS (
			INSERT INTO replication_queue_job_lock (job_id, lock_id, triggered_at)
			SELECT job.id, job.lock_id, NOW() AT TIME ZONE 'UTC'
			FROM job
			RETURNING lock_id
		)
		, acquire_lock AS (
			UPDATE replication_queue_lock AS lock
			SET acquired = TRUE
			FROM track_job_lock AS tracked
			WHERE lock.id = tracked.lock_id
		)
		SELECT id, state, created_at, updated_at, lock_id, attempt, job, meta
		FROM job
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

func (rq PostgresReplicationEventQueue) Acknowledge(ctx context.Context, state JobState, ids []uint64) ([]uint64, error) {
	// When `Acknowledge` method is called:
	//  1. The list of event `id`s and corresponding <lock>s retrieved from `replication_queue` table as passed in by the
	//     user `ids` could not exist in the table or the `state` of the event could differ from `in_progress` (it is
	//     possible to acknowledge only events previously fetched by the `Dequeue` method)
	//  2. Based on the list fetched on previous step the delete is executed on the `replication_queue` table. In case the
	//     new state for the entry is 'dead' it will be just deleted, but if the new state is 'completed' the event will
	//     be delete as well, but all events similar to it (events for the same repository with same change type and a source)
	//     that were created before processed events were queued for processing will also be deleted.
	//     In case the new state is something different ('failed') the event will be updated only with a new state.
	//     It returns a list of event `id`s and corresponding <lock>s of the affected events during this delete/update process.
	//  3. The removal of records in `replication_queue_job_lock` table happens that were created by step 4. of `Dequeue`
	//     method call.
	//  4. Acquisition state of <lock>s in `replication_queue_lock` table updated by comparing amount of existing bindings
	//     in `replication_queue_lock` table for the <lock> to amount of removed bindings done on the 3. for the <lock>
	//     and if amount is the same the <lock> is free and column `acquired` assigned `FALSE` value, so this <lock> can
	//     be used in the `Dequeue` method to retrieve other events. If amounts don't match no update happens and <lock>
	//     remains acquired until all events are acknowledged (binding records removed from the `replication_queue_job_lock`
	//     table).

	if len(ids) == 0 {
		return nil, nil
	}

	if err := allowToAck(state); err != nil {
		return nil, err
	}

	pqIDs := make(pq.Int64Array, len(ids))
	for i, id := range ids {
		pqIDs[i] = int64(id)
	}

	query := `
		WITH existing AS (
			SELECT id, lock_id, updated_at, job
			FROM replication_queue
			WHERE id = ANY($1)
			AND state = 'in_progress'
			FOR UPDATE
		)
		, deleted AS (
			DELETE FROM replication_queue AS queue
			USING existing
			WHERE ($2::REPLICATION_JOB_STATE = 'dead' AND existing.id = queue.id) OR (
				$2::REPLICATION_JOB_STATE = 'completed'
				AND (existing.id = queue.id OR (
					-- this is an optimization to omit events that won't make any effect as the same event
					-- was just applied, so we acknowledge similar events:
					-- only not yet touched events (no attempts to process it)
					queue.state = 'ready'
					-- and they were created before current event was consumed for processing
					AND queue.created_at < existing.updated_at
					-- they are for the exact same repository
					AND queue.lock_id = existing.lock_id
					-- and created to apply exact same replication operation (gc, update, ...)
					AND queue.job->>'change' = existing.job->>'change'
					-- from the same source storage (if applicable, as 'gc' has no source)
					AND COALESCE(queue.job->>'source_node_storage', '') = COALESCE(existing.job->>'source_node_storage', ''))
				)
			)
			RETURNING queue.id, queue.lock_id
		)
		, updated AS (
			UPDATE replication_queue AS queue
			SET state = $2::REPLICATION_JOB_STATE,
				updated_at = NOW() AT TIME ZONE 'UTC'
			FROM existing
			WHERE existing.id = queue.id
			RETURNING queue.id, queue.lock_id
		)
		, removed_job_lock AS (
			DELETE FROM replication_queue_job_lock AS job_lock
			USING (SELECT * FROM deleted UNION SELECT * FROM updated) AS to_release
			WHERE job_lock.job_id = to_release.id AND job_lock.lock_id = to_release.lock_id
			RETURNING to_release.lock_id
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
	rows, err := rq.qc.QueryContext(ctx, query, pqIDs, state)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var acknowledged glsql.Uint64Provider
	if err := glsql.ScanAll(rows, &acknowledged); err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}

	return acknowledged.Values(), rows.Err()
}

// StartHealthUpdate starts periodical update of the event's health identifier.
// The events with fresh health identifier won't be considered as stale.
// The health update will be executed on each new entry received from trigger channel passed in.
// It is a blocking call that is managed by the passed in context.
func (rq PostgresReplicationEventQueue) StartHealthUpdate(ctx context.Context, trigger <-chan time.Time, events []ReplicationEvent) error {
	if len(events) == 0 {
		return nil
	}

	jobIDs := make(pq.Int64Array, len(events))
	lockIDs := make(pq.StringArray, len(events))
	for i := range events {
		jobIDs[i] = int64(events[i].ID)
		lockIDs[i] = events[i].LockID
	}

	query := `
		UPDATE replication_queue_job_lock
		SET triggered_at = NOW() AT TIME ZONE 'UTC'
		WHERE (job_id, lock_id) IN (SELECT UNNEST($1::BIGINT[]), UNNEST($2::TEXT[]))`

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-trigger:
			res, err := rq.qc.ExecContext(ctx, query, jobIDs, lockIDs)
			if err != nil {
				if pqError, ok := err.(*pq.Error); ok && pqError.Code.Name() == "query_canceled" {
					return nil
				}
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return nil
				}
				return err
			}

			affected, err := res.RowsAffected()
			if err != nil {
				return err
			}

			if affected == 0 {
				return nil
			}
		}
	}
}

// AcknowledgeStale moves replication events that are 'in_progress' state for too long (more then staleAfter)
// into the next state:
//   'failed' - in case it has more attempts to be executed
//   'dead' - in case it has no more attempts to be executed
// The job considered 'in_progress' if it has corresponding entry in the 'replication_queue_job_lock' table.
// When moving from 'in_progress' to other state the entry from 'replication_queue_job_lock' table will be
// removed and entry in the 'replication_queue_lock' will be updated if needed (release of the lock).
func (rq PostgresReplicationEventQueue) AcknowledgeStale(ctx context.Context, staleAfter time.Duration) error {
	query := `
		WITH stale_job_lock AS (
			DELETE FROM replication_queue_job_lock WHERE triggered_at < NOW() - INTERVAL '1 MILLISECOND' * $1
			RETURNING job_id, lock_id
		)
		, update_job AS (
			UPDATE replication_queue AS queue
			SET state = (CASE WHEN attempt >= 1 THEN 'failed' ELSE 'dead' END)::REPLICATION_JOB_STATE
			FROM stale_job_lock
			WHERE stale_job_lock.job_id = queue.id
			RETURNING queue.id, queue.lock_id
		)
		UPDATE replication_queue_lock
		SET acquired = FALSE
		WHERE id IN (
			SELECT existing.lock_id
			FROM (SELECT lock_id, COUNT(*) AS amount FROM stale_job_lock GROUP BY lock_id) AS removed
			JOIN (
				SELECT lock_id, COUNT(*) AS amount
				FROM replication_queue_job_lock
				WHERE lock_id IN (SELECT lock_id FROM stale_job_lock)
				GROUP BY lock_id
			) AS existing ON removed.lock_id = existing.lock_id AND removed.amount = existing.amount
		)`
	_, err := rq.qc.ExecContext(ctx, query, staleAfter.Milliseconds())
	if err != nil {
		return fmt.Errorf("exec acknowledge stale: %w", err)
	}

	return nil
}
