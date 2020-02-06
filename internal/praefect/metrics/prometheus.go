package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	promconfig "gitlab.com/gitlab-org/gitaly/internal/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
)

// RegisterReplicationLatency creates and registers a prometheus histogram
// to observe replication latency times
func RegisterReplicationLatency(conf promconfig.Config) (Histogram, error) {
	replicationLatency := prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "gitaly",
			Subsystem: "praefect",
			Name:      "replication_latency",
			Buckets:   conf.GRPCLatencyBuckets,
		},
	)

	return replicationLatency, prometheus.Register(replicationLatency)
}

// RegisterReplicationJobsInFlight creates and registers a gauge
// to track the size of the replication queue
func RegisterReplicationJobsInFlight() (Gauge, error) {
	replicationJobsInFlight := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "gitaly",
			Subsystem: "praefect",
			Name:      "replication_jobs",
		},
	)
	return replicationJobsInFlight, prometheus.Register(replicationJobsInFlight)
}

// Gauge is a subset of a prometheus Gauge
type Gauge interface {
	Inc()
	Dec()
}

// Histogram is a subset of a prometheus Histogram
type Histogram interface {
	Observe(float64)
}

var once sync.Once

var (
	// ProxyTime monitors the latency added by praefect to each request
	ProxyTime prometheus.Histogram
)

// RegisterProxyTime registers praefect prometheus metrics
func RegisterProxyTime(c config.Config) {
	once.Do(func() { registerProxyTime(c) })
}

func registerProxyTime(c config.Config) {
	ProxyTime = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "praefect_proxy_time",
		Help:    "Latency added by praefect",
		Buckets: c.Prometheus.GRPCLatencyBuckets,
	})

	prometheus.MustRegister(ProxyTime)
}
