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

// RepositoryStoreCollector collects metrics from the RepositoryStore.
type RepositoryStoreCollector struct {
	log              logrus.FieldLogger
	db               glsql.Querier
	virtualStorages  []string
	repositoryScoped bool
}

// NewRepositoryStoreCollector returns a new collector.
func NewRepositoryStoreCollector(log logrus.FieldLogger, virtualStorages []string, db glsql.Querier, repositoryScoped bool) *RepositoryStoreCollector {
	return &RepositoryStoreCollector{
		log:              log.WithField("component", "RepositoryStoreCollector"),
		db:               db,
		virtualStorages:  virtualStorages,
		repositoryScoped: repositoryScoped,
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
	rows, err := c.db.QueryContext(ctx, `
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
WHERE latest_generation > COALESCE(generation, -1)
GROUP BY virtual_storage
	`, pq.StringArray(c.virtualStorages), c.repositoryScoped)
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
