package reconciler

import (
	"context"
	"fmt"

	"github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v13/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/nodes"
)

// Reconciler implements reconciliation logic for repairing outdated repository replicas.
type Reconciler struct {
	log                              logrus.FieldLogger
	db                               glsql.Querier
	nm                               nodes.Manager
	storages                         map[string][]string
	reconciliationSchedulingDuration prometheus.Histogram
	reconciliationJobsTotal          *prometheus.CounterVec
	// handleError is called with a possible error from reconcile.
	// If it returns an error, Run stops and returns with the error.
	handleError func(error) error
}

// NewReconciler returns a new Reconciler for repairing outdated repositories.
func NewReconciler(log logrus.FieldLogger, db glsql.Querier, nm nodes.Manager, storages map[string][]string, buckets []float64) *Reconciler {
	log = log.WithField("component", "reconciler")

	r := &Reconciler{
		log:      log,
		db:       db,
		nm:       nm,
		storages: storages,
		reconciliationSchedulingDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "gitaly_praefect_reconciliation_scheduling_seconds",
			Help:    "The time spent performing a single reconciliation scheduling run.",
			Buckets: buckets,
		}),
		reconciliationJobsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gitaly_praefect_reconciliation_jobs_total",
			Help: "Number of reconciliation jobs scheduled.",
		}, []string{"virtual_storage", "source_storage", "target_storage"}),
		handleError: func(err error) error {
			log.WithError(err).Error("automatic reconciliation failed")
			return nil
		},
	}

	// create the timeseries prior to having observations
	for vs, storages := range r.storages {
		for i := range storages {
			for j := range storages {
				if i == j {
					// source and the target can't be the same
					continue
				}

				r.reconciliationJobsTotal.WithLabelValues(vs, storages[i], storages[j])
			}
		}
	}

	return r
}

func (r *Reconciler) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(r, ch)
}

func (r *Reconciler) Collect(ch chan<- prometheus.Metric) {
	r.reconciliationSchedulingDuration.Collect(ch)
	r.reconciliationJobsTotal.Collect(ch)
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

// reconcile schedules replication jobs to outdated repositories using random up to date
// replica of the repository as the source. Only outdated repositories on the healthy storages
// are targeted to avoid unnecessarily scheduling jobs that can't be completed. Source node
// used is a random healthy storage that contains the latest generation of the repository. If
// there is an `update` type replication job targeting the outdated repository, no replication
// job will be scheduled to avoid queuing up multiple redundant jobs.
func (r *Reconciler) reconcile(ctx context.Context) error {
	defer prometheus.NewTimer(r.reconciliationSchedulingDuration).ObserveDuration()

	var virtualStorages []string
	var storages []string
	for virtualStorage := range r.storages {
		if len(r.storages[virtualStorage]) < 2 {
			continue
		}

		shard, err := r.nm.GetShard(virtualStorage)
		if err != nil {
			r.log.WithFields(logrus.Fields{
				"virtual_storage": virtualStorage,
				"error":           err,
			}).Error("unable to include virtual storage for reconciliation")
			continue
		}

		healthyNodes := shard.GetHealthySecondaries()
		if shard.Primary.IsHealthy() {
			healthyNodes = append(healthyNodes, shard.Primary)
		}

		if len(healthyNodes) < 2 {
			// minimum two healthy storages within a virtual stoage needed for valid
			// replication source and target
			r.log.WithField("virtual_storage", virtualStorage).Info("reconciliation skipped for virtual storage due to not having enough healthy storages")
			continue
		}

		for _, node := range healthyNodes {
			virtualStorages = append(virtualStorages, virtualStorage)
			storages = append(storages, node.GetStorage())
		}
	}

	if len(virtualStorages) == 0 {
		return nil
	}

	rows, err := r.db.QueryContext(ctx, `
WITH healthy_storages AS (
    SELECT unnest($1::text[]) AS virtual_storage,
           unnest($2::text[]) AS storage
), reconciliation_jobs AS (
	INSERT INTO replication_queue (lock_id, job, meta)
	SELECT
		(virtual_storage || '|' || target_node_storage || '|' || relative_path),
		to_jsonb(reconciliation_jobs),
		jsonb_build_object('correlation_id', encode(random()::text::bytea, 'base64'))
	FROM (
		SELECT DISTINCT ON (virtual_storage, relative_path, target_node_storage)
			virtual_storage,
			relative_path,
			source_node_storage,
			target_node_storage,
			'update' AS change
		FROM (
			SELECT virtual_storage, relative_path, storage AS target_node_storage
			FROM repositories
			JOIN healthy_storages USING (virtual_storage)
			LEFT JOIN storage_repositories USING (virtual_storage, relative_path, storage)
			WHERE COALESCE(storage_repositories.generation != repositories.generation, true)
			ORDER BY virtual_storage, relative_path
		) AS unhealthy_repositories
		JOIN (
			SELECT virtual_storage, relative_path, storage AS source_node_storage
			FROM storage_repositories
			JOIN healthy_storages USING (virtual_storage, storage)
			JOIN repositories USING (virtual_storage, relative_path, generation)
			ORDER BY virtual_storage, relative_path
		) AS healthy_repositories USING (virtual_storage, relative_path)
		WHERE NOT EXISTS (
			SELECT true
			FROM replication_queue
			WHERE state IN ('ready', 'in_progress', 'failed')
			AND job->>'change' = 'update'
			AND job->>'virtual_storage' = virtual_storage
			AND job->>'relative_path' = relative_path
			AND job->>'target_node_storage' = target_node_storage
		)
		ORDER BY virtual_storage, relative_path, target_node_storage, random()
	) AS reconciliation_jobs
	RETURNING lock_id, meta, job
), locks AS (
	INSERT INTO replication_queue_lock(id)
	SELECT lock_id
	FROM reconciliation_jobs
	ON CONFLICT (id) DO NOTHING
)

SELECT
	meta->>'correlation_id',
	job->>'virtual_storage',
	job->>'relative_path',
	job->>'source_node_storage',
	job->>'target_node_storage'
FROM reconciliation_jobs
`, pq.StringArray(virtualStorages), pq.StringArray(storages))
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}

	defer func() {
		if err := rows.Close(); err != nil {
			r.log.WithError(err).Error("error closing rows")
		}
	}()

	type job struct {
		CorrelationID  string `json:"correlation_id"`
		VirtualStorage string `json:"virtual_storage"`
		RelativePath   string `json:"relative_path"`
		SourceStorage  string `json:"source_storage"`
		TargetStorage  string `json:"target_storage"`
	}

	var jobs []job
	for rows.Next() {
		var j job
		if err := rows.Scan(
			&j.CorrelationID,
			&j.VirtualStorage,
			&j.RelativePath,
			&j.SourceStorage,
			&j.TargetStorage,
		); err != nil {
			return fmt.Errorf("scan: %w", err)
		}

		r.reconciliationJobsTotal.WithLabelValues(j.VirtualStorage, j.SourceStorage, j.TargetStorage).Inc()

		jobs = append(jobs, j)
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("rows.Err: %w", err)
	}

	if len(jobs) > 0 {
		r.log.WithField("scheduled_jobs", jobs).Info("reconciliation jobs scheduled")
	} else {
		r.log.Debug("reconciliation did not result in any scheduled jobs")
	}

	return nil
}
