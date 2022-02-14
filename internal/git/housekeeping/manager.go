package housekeeping

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
)

// Manager is a housekeeping manager. It is supposed to handle housekeeping tasks for repositories
// such as the cleanup of unneeded files and optimizations for the repository's data structures.
type Manager interface {
	// CleanStaleData removes any stale data in the repository.
	CleanStaleData(context.Context, *localrepo.Repo) error
	// OptimizeRepository optimizes the repository's data structures such that it can be more
	// efficiently served.
	OptimizeRepository(context.Context, *localrepo.Repo) error
}

// RepositoryManager is an implementation of the Manager interface.
type RepositoryManager struct {
	txManager transaction.Manager

	tasksTotal   *prometheus.CounterVec
	tasksLatency *prometheus.HistogramVec
}

// NewManager creates a new RepositoryManager.
func NewManager(txManager transaction.Manager) *RepositoryManager {
	return &RepositoryManager{
		txManager: txManager,

		tasksTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gitaly_housekeeping_tasks_total",
				Help: "Total number of housekeeping tasks performed in the repository",
			},
			[]string{"housekeeping_task"},
		),
		tasksLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "gitaly_housekeeping_tasks_latency",
				Help: "Latency of the housekeeping tasks performed",
			},
			[]string{"housekeeping_task"},
		),
	}
}

// Describe is used to describe Prometheus metrics.
func (m *RepositoryManager) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(m, descs)
}

// Collect is used to collect Prometheus metrics.
func (m *RepositoryManager) Collect(metrics chan<- prometheus.Metric) {
	m.tasksTotal.Collect(metrics)
	m.tasksLatency.Collect(metrics)
}
