package reconciler

import (
	"context"
	"fmt"

	"github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/advisorylock"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
)

const logBatchSize = 25

// Reconciler implements reconciliation logic for repairing outdated repository replicas.
type Reconciler struct {
	log                              logrus.FieldLogger
	db                               glsql.Querier
	hc                               praefect.HealthChecker
	storages                         map[string][]string
	reconciliationSchedulingDuration prometheus.Histogram
	// handleError is called with a possible error from reconcile.
	// If it returns an error, Run stops and returns with the error.
	handleError func(error) error
}

// NewReconciler returns a new Reconciler for repairing outdated repositories.
func NewReconciler(log logrus.FieldLogger, db glsql.Querier, hc praefect.HealthChecker, storages map[string][]string, buckets []float64) *Reconciler {
	log = log.WithField("component", "reconciler")

	r := &Reconciler{
		log:      log,
		db:       db,
		hc:       hc,
		storages: storages,
		reconciliationSchedulingDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "gitaly_praefect_reconciliation_scheduling_seconds",
			Help:    "The time spent performing a single reconciliation scheduling run.",
			Buckets: buckets,
		}),
		handleError: func(err error) error {
			log.WithError(err).Error("automatic reconciliation failed")
			return nil
		},
	}

	return r
}

func (r *Reconciler) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(r, ch)
}

func (r *Reconciler) Collect(ch chan<- prometheus.Metric) {
	r.reconciliationSchedulingDuration.Collect(ch)
}

// Run reconciles on each tick the Ticker emits. Run returns
// when the context is canceled, returning the error from the context.
func (r *Reconciler) Run(ctx context.Context, ticker helper.Ticker) error {
	r.log.WithField("storages", r.storages).Info("automatic reconciler started")
	defer r.log.Info("automatic reconciler stopped")

	defer ticker.Stop()

	for {
		ticker.Reset()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C():
			if err := r.reconcile(ctx); err != nil {
				if err := r.handleError(err); err != nil {
					return err
				}
			}
		}
	}
}

// job is an internal type for formatting log messages
type job struct {
	Change         string  `json:"change"`
	CorrelationID  string  `json:"correlation_id"`
	VirtualStorage string  `json:"virtual_storage"`
	RelativePath   string  `json:"relative_path"`
	SourceStorage  *string `json:"source_storage,omitempty"`
	TargetStorage  string  `json:"target_storage"`
}

// reconcile schedules replication jobs to fix any discrepancies in how the expected state of the
// virtual storage is compared to the actual state.
//
// It currently handles fixing two discrepancies:
//
// 1. Assigned storage having an outdated replica of a repository. This is fixed by scheduling
//    an `update` type job from any healthy storage with an up to date replica. These are only
//    scheduled if there is no other active `update` type job targeting the outdated replica.
// 2. Unassigned storage having an unnecessary replica. This is fixed by scheduling a `delete_replica`
//    type job to remove the unneeded replica from the storage. These are only scheduled if all assigned
//    storages are up to date and the replica is not used as a source or target storage in any other job.
//    Only one job of this type is allowed to be queued for a given repository at a time. This is to avoid
//    deleting too many replicas if assignments are changed while the jobs are queued.
//
// The fixes are only scheduled if the target node is healthy, and if there is a healthy source node
// available should the job need one.
//
// If the repository has no assignments set, reconcile falls back to considering every storage in the
// virtual storage as assigned. As all storages are considered assigned if no assignments exist, no
// `delete_replica` jobs are scheduled when assignments are not explicitly set.
func (r *Reconciler) reconcile(ctx context.Context) error {
	defer prometheus.NewTimer(r.reconciliationSchedulingDuration).ObserveDuration()

	var virtualStorages []string
	var storages []string

	for virtualStorage, healthyStorages := range r.hc.HealthyNodes() {
		if len(healthyStorages) < 2 {
			// minimum two healthy storages within a virtual stoage needed for valid
			// replication source and target
			r.log.WithField("virtual_storage", virtualStorage).Info("reconciliation skipped for virtual storage due to not having enough healthy storages")
			continue
		}

		for _, storage := range healthyStorages {
			virtualStorages = append(virtualStorages, virtualStorage)
			storages = append(storages, storage)
		}
	}

	if len(virtualStorages) == 0 {
		return nil
	}

	rows, err := r.db.QueryContext(ctx, `
WITH reconciliation_lock AS (
	SELECT pg_try_advisory_xact_lock($1) AS acquired
),

healthy_storages AS (
    SELECT unnest($2::text[]) AS virtual_storage,
           unnest($3::text[]) AS storage
),

delete_jobs AS (
	SELECT DISTINCT ON (virtual_storage, relative_path)
		virtual_storage,
		relative_path,
		storage
	FROM storage_repositories
	JOIN healthy_storages USING (virtual_storage, storage)
	WHERE (
		-- Only unassigned repositories should be targeted for deletion. If no assignment exists,
		-- every storage is considered assigned, thus no deletion would be scheduled.
		SELECT COUNT(storage) > 0 AND COUNT(storage) FILTER (WHERE storage = storage_repositories.storage) = 0
		FROM repository_assignments
		WHERE virtual_storage = storage_repositories.virtual_storage
		AND   relative_path   = storage_repositories.relative_path
	)
	AND generation <= (
		-- Check whether the replica's generation is equal or lower than the generation of every assigned
		-- replica of the repository. If so, then it is eligible for deletion.
		SELECT MIN(COALESCE(generation, -1))
		FROM repository_assignments
		FULL JOIN storage_repositories AS sr USING (virtual_storage, relative_path, storage)
		WHERE virtual_storage = storage_repositories.virtual_storage
		AND relative_path = storage_repositories.relative_path
	) AND NOT EXISTS (
		-- Ensure the replica is not used as target or source in any scheduled job. This is to avoid breaking
		-- any already scheduled jobs.
		SELECT FROM replication_queue
		WHERE job->>'virtual_storage' = virtual_storage
		AND   job->>'relative_path' = relative_path
		AND (
				job->>'source_node_storage' = storage
			OR 	job->>'target_node_storage' = storage
		)
		AND state NOT IN ('completed', 'dead', 'cancelled')
	) AND NOT EXISTS (
		-- Ensure there are no other scheduled 'delete_replica' type jobs for the repository. Performing rapid
		-- repository_assignments could cause the reconciler to schedule deletion against all replicas. To avoid this,
		-- we do not allow more than one 'delete_replica' job to be active at any given time.
		SELECT FROM replication_queue
		WHERE state NOT IN ('completed', 'cancelled', 'dead')
		AND job->>'virtual_storage' = virtual_storage
		AND job->>'relative_path' = relative_path
		AND job->>'change' = 'delete_replica'
	)
),

update_jobs AS (
	SELECT DISTINCT ON (virtual_storage, relative_path, target_node_storage)
		virtual_storage,
		relative_path,
		source_node_storage,
		target_node_storage
	FROM (
		SELECT virtual_storage, relative_path, storage AS target_node_storage
		FROM repositories
		JOIN healthy_storages USING (virtual_storage)
		LEFT JOIN storage_repositories USING (virtual_storage, relative_path, storage)
		WHERE COALESCE(storage_repositories.generation != repositories.generation, true)
		AND (
			-- If assignments exist for the repository, only the assigned storages are targeted for replication.
			-- If no assignments exist, every healthy node is targeted for replication.
			SELECT COUNT(storage) = 0 OR COUNT(storage) FILTER (WHERE storage = healthy_storages.storage) = 1
			FROM repository_assignments
			WHERE virtual_storage = repositories.virtual_storage
			AND   relative_path   = repositories.relative_path
		)
		ORDER BY virtual_storage, relative_path
	) AS unhealthy_repositories
	JOIN (
		SELECT virtual_storage, relative_path, storage AS source_node_storage
		FROM storage_repositories
		JOIN healthy_storages USING (virtual_storage, storage)
		JOIN repositories USING (virtual_storage, relative_path, generation)
		WHERE NOT EXISTS (
			SELECT FROM replication_queue
			WHERE state NOT IN ('completed', 'cancelled', 'dead')
			AND job->>'virtual_storage' = virtual_storage
			AND job->>'relative_path' = relative_path
			AND job->>'target_node_storage' = storage
			AND job->>'change' = 'delete_replica'
		)
		ORDER BY virtual_storage, relative_path
	) AS healthy_repositories USING (virtual_storage, relative_path)
	WHERE NOT EXISTS (
		SELECT FROM replication_queue
		WHERE state NOT IN ('completed', 'cancelled', 'dead')
		AND job->>'virtual_storage' = virtual_storage
		AND job->>'relative_path' = relative_path
		AND job->>'target_node_storage' = target_node_storage
		AND job->>'change' = 'update'
	)
	ORDER BY virtual_storage, relative_path, target_node_storage, random()
),

reconciliation_jobs AS (
	INSERT INTO replication_queue (lock_id, job, meta)
	SELECT
		(virtual_storage || '|' || target_node_storage || '|' || relative_path),
		to_jsonb(reconciliation_jobs),
		jsonb_build_object('correlation_id', encode(random()::text::bytea, 'base64'))
	FROM (
		SELECT
			virtual_storage,
			relative_path,
			source_node_storage,
			target_node_storage,
			'update' AS change
		FROM update_jobs
			UNION ALL
		SELECT
			virtual_storage,
			relative_path,
			NULL AS source_node_storage,
			storage AS target_node_storage,
			'delete_replica' AS change
		FROM delete_jobs
	) AS reconciliation_jobs
	-- only perform inserts if we managed to acquire the lock as otherwise
	-- we'd schedule duplicate jobs
	WHERE ( SELECT acquired FROM reconciliation_lock )
	RETURNING lock_id, meta, job
),

create_locks AS (
	INSERT INTO replication_queue_lock(id)
	SELECT lock_id
	FROM reconciliation_jobs
	ON CONFLICT (id) DO NOTHING
)

SELECT
	meta->>'correlation_id',
	job->>'change',
	job->>'virtual_storage',
	job->>'relative_path',
	job->>'source_node_storage',
	job->>'target_node_storage'
FROM reconciliation_jobs
`, advisorylock.Reconcile, pq.StringArray(virtualStorages), pq.StringArray(storages))
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}

	defer func() {
		if err := rows.Close(); err != nil {
			r.log.WithError(err).Error("error closing rows")
		}
	}()

	jobs := make([]job, 0, logBatchSize)

	for rows.Next() {
		var j job
		if err := rows.Scan(
			&j.CorrelationID,
			&j.Change,
			&j.VirtualStorage,
			&j.RelativePath,
			&j.SourceStorage,
			&j.TargetStorage,
		); err != nil {
			return fmt.Errorf("scan: %w", err)
		}

		jobs = append(jobs, j)
		if len(jobs) == logBatchSize {
			r.logJobs(jobs)
			jobs = jobs[:0]
		}
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("rows.Err: %w", err)
	}

	if len(jobs) > 0 {
		r.logJobs(jobs)
	} else {
		r.log.Debug("reconciliation did not result in any scheduled jobs")
	}

	return nil
}

func (r *Reconciler) logJobs(jobs []job) {
	r.log.WithField("scheduled_jobs", jobs).Info("reconciliation jobs scheduled")
}
