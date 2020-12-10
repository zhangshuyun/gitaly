package datastore

import (
	"context"
	"fmt"

	"github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore/glsql"
)

var descReadOnlyRepositories = prometheus.NewDesc(
	"gitaly_praefect_read_only_repositories",
	"Number of repositories in read-only mode within a virtual storage.",
	[]string{"virtual_storage"},
	nil,
)

type HealthChecker interface {
	HealthyNodes() map[string][]string
}

// RepositoryStoreCollector collects metrics from the RepositoryStore.
type RepositoryStoreCollector struct {
	log              logrus.FieldLogger
	db               glsql.Querier
	virtualStorages  []string
	repositoryScoped bool
	healthChecker    HealthChecker
}

// NewRepositoryStoreCollector returns a new collector.
func NewRepositoryStoreCollector(log logrus.FieldLogger, virtualStorages []string, db glsql.Querier, repositoryScoped bool, hc HealthChecker) *RepositoryStoreCollector {
	return &RepositoryStoreCollector{
		log:              log.WithField("component", "RepositoryStoreCollector"),
		db:               db,
		virtualStorages:  virtualStorages,
		repositoryScoped: repositoryScoped,
		healthChecker:    hc,
	}
}

func (c *RepositoryStoreCollector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, ch)
}

func (c *RepositoryStoreCollector) Collect(ch chan<- prometheus.Metric) {
	readOnlyCounts, err := c.queryMetrics(context.TODO())
	if err != nil {
		c.log.WithError(err).Error("failed collecting read-only repository count metric")
		return
	}

	for _, vs := range c.virtualStorages {
		ch <- prometheus.MustNewConstMetric(descReadOnlyRepositories, prometheus.GaugeValue, float64(readOnlyCounts[vs]), vs)
	}
}

// queryMetrics queries the number of read-only repositories from the database.
// A repository is in read-only mode when its primary storage is not on the latest
// generation. If `repositoryScoped` was passed set to true in the constructor, the
// query considers repository specific primaries. Otherwise, virtual storage scoped
// primaries are considered.
func (c *RepositoryStoreCollector) queryMetrics(ctx context.Context) (map[string]int, error) {
	var virtualStorages, storages []string
	for virtualStorage, stors := range c.healthChecker.HealthyNodes() {
		for _, storage := range stors {
			virtualStorages = append(virtualStorages, virtualStorage)
			storages = append(storages, storage)
		}
	}

	// The query differs slightly depending on whether repository specific primaries are enabled or not.
	//
	// For virtual storage scoped primaries, we fetch the primaries from the `shard_primaries.node_name`
	// column. We then check whether the primary storage is on the latest generation. If not, we add
	// it to the read-only count.
	//
	// When used with repository specific primaries, the repository's primary is taken from the
	// `repositories.primary` column. To account for possibility of lazy elections, the query does
	// not immediately count outdated primaries as read-only. It additionally checks whether the primary
	// node is unhealthy. If it is not, then the primary is counted as read-only since the elector would
	// not fail over from a healthy primary. If the primary is unhealthy, the elector could fail over to
	// another node when the primary is needed. The query checks whethere there is a healthy and assigned
	// in-sync node available. If so, the elector would fail over to it on the next request, thus the
	// repository would not be in read-only mode.
	rows, err := c.db.QueryContext(ctx, `
WITH healthy_storages AS (
	SELECT unnest($3::text[]) AS virtual_storage, unnest($4::text[]) AS storage
)

SELECT virtual_storage, COUNT(*)
FROM (
	SELECT
		virtual_storage,
		relative_path,
		CASE WHEN $2
			THEN "primary"
			ELSE node_name
		END	AS storage
	FROM repositories
	LEFT JOIN shard_primaries ON NOT $2 AND shard_name = virtual_storage AND NOT demoted
	WHERE virtual_storage = ANY($1)
) AS repositories
JOIN (
	SELECT virtual_storage, relative_path, max(generation) AS latest_generation
	FROM storage_repositories
	GROUP BY virtual_storage, relative_path
) AS latest_generations USING (virtual_storage, relative_path)
LEFT JOIN storage_repositories USING (virtual_storage, relative_path, storage)
WHERE (
		latest_generation > COALESCE(generation, -1)
	OR ($2 AND (virtual_storage, storage) NOT IN ( SELECT virtual_storage, storage FROM healthy_storages )))
AND (
		NOT $2
	OR (virtual_storage, storage) IN ( SELECT virtual_storage, storage FROM healthy_storages )
	OR NOT EXISTS (
		SELECT 1
		FROM storage_repositories AS primary_candidate
		JOIN healthy_storages USING (virtual_storage, storage)
		WHERE primary_candidate.virtual_storage = repositories.virtual_storage
		AND   primary_candidate.relative_path = repositories.relative_path
		AND   primary_candidate.generation = latest_generation
		AND (
			SELECT COUNT(*) = 0 OR COUNT(*) FILTER (WHERE storage = primary_candidate.storage) = 1
			FROM repository_assignments
			WHERE repository_assignments.virtual_storage = primary_candidate.virtual_storage
			AND repository_assignments.relative_path = primary_candidate.relative_path
		)
	)
)
GROUP BY virtual_storage
	`,
		pq.StringArray(c.virtualStorages),
		c.repositoryScoped,
		pq.StringArray(virtualStorages),
		pq.StringArray(storages),
	)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	vsReadOnly := make(map[string]int)
	for rows.Next() {
		var vs string
		var count int

		if err := rows.Scan(&vs, &count); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		vsReadOnly[vs] = count
	}

	return vsReadOnly, rows.Err()
}
