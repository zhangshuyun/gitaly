package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
)

var once sync.Once

var (
	// ProxyTime monitors the latency added by praefect to each request
	ProxyTime prometheus.Histogram
)

// Register registers praefect prometheus metrics
func Register(c config.Config) {
	once.Do(func() { register(c) })
}

func register(c config.Config) {
	ProxyTime = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "praefect_proxy_time",
		Help:    "Latency added by praefect",
		Buckets: prometheus.LinearBuckets(100000, 100000, 20),
	})

	prometheus.MustRegister(ProxyTime)
}
