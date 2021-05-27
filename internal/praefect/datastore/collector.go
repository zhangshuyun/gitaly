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
	log             logrus.FieldLogger
	db              glsql.Querier
	virtualStorages []string
}

// NewRepositoryStoreCollector returns a new collector.
func NewRepositoryStoreCollector(log logrus.FieldLogger, virtualStorages []string, db glsql.Querier) *RepositoryStoreCollector {
	return &RepositoryStoreCollector{
		log:             log.WithField("component", "RepositoryStoreCollector"),
		db:              db,
		virtualStorages: virtualStorages,
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

// queryMetrics queries the number of unavailable repositories from the database.
// A repository is unavailable when it has no replicas that can act as a primary, indicating
// they are either unhealthy or out of date.
func (c *RepositoryStoreCollector) queryMetrics(ctx context.Context) (map[string]int, error) {
	rows, err := c.db.QueryContext(ctx, `
SELECT virtual_storage, COUNT(*)
FROM repositories
WHERE NOT EXISTS (
	SELECT FROM valid_primaries
	WHERE valid_primaries.virtual_storage = repositories.virtual_storage
	AND   valid_primaries.relative_path   = repositories.relative_path
) AND repositories.virtual_storage = ANY($1)
GROUP BY virtual_storage
	`, pq.StringArray(c.virtualStorages))
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
