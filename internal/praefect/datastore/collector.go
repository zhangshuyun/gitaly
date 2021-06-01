package datastore

import (
	"context"
	"fmt"

	"github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
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
// generation.
//
// There are two variants on the query:
//
// 1. virtualStorageScopedQuery considers virtual storage scoped primaries. Virtual storage's
//    primary is stored in the `shard_primaries` table in the `node_name` column.
// 2. repositoryScopedQuery considers repository specific primaries. Repository scoped
//    primaries are stored in the `primary` column in the `repositories` table.
//
// Both queries cross-reference the `repositories` and `storage_repositories` tables
// to see if the primary storage of the repository is on the latest generation. If not,
// it's added to the returned count.
//
// The query operating on virtual storage scoped primaries will be dropped once the migration
// to repository scoped primaries is finished.
func (c *RepositoryStoreCollector) queryMetrics(ctx context.Context) (map[string]int, error) {
	const virtualStorageScopedQuery = `
SELECT repositories.virtual_storage, COUNT(*)
FROM repositories
LEFT JOIN shard_primaries ON
	shard_primaries.shard_name = repositories.virtual_storage AND
	shard_primaries.demoted = false
LEFT JOIN storage_repositories ON
	repositories.virtual_storage = storage_repositories.virtual_storage AND
	repositories.relative_path = storage_repositories.relative_path AND
	shard_primaries.node_name = storage_repositories.storage
WHERE
	COALESCE(storage_repositories.generation, -1) < repositories.generation AND
	repositories.virtual_storage = ANY($1)
GROUP BY repositories.virtual_storage;
	`

	const repositoryScopedQuery = `
SELECT repositories.virtual_storage, COUNT(*)
FROM repositories
LEFT JOIN storage_repositories ON
	repositories.virtual_storage = storage_repositories.virtual_storage AND
	repositories.relative_path = storage_repositories.relative_path AND
	repositories.primary = storage_repositories.storage
WHERE
	COALESCE(storage_repositories.generation, -1) < repositories.generation AND
	repositories.virtual_storage = ANY($1)
GROUP BY repositories.virtual_storage
`

	query := virtualStorageScopedQuery
	if c.repositoryScoped {
		query = repositoryScopedQuery
	}

	rows, err := c.db.QueryContext(ctx, query, pq.StringArray(c.virtualStorages))
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
