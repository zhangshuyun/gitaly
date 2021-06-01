package prometheus

import (
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/middleware/limithandler"
)

// Config contains additional configuration data for prometheus
type Config struct {
	GRPCLatencyBuckets []float64 `toml:"grpc_latency_buckets"`
}

// Configure configures latency buckets for prometheus timing histograms
func (c *Config) Configure() {
	if len(c.GRPCLatencyBuckets) == 0 {
		return
	}

	log.WithField("latencies", c.GRPCLatencyBuckets).Info("grpc prometheus histograms enabled")

	grpc_prometheus.EnableHandlingTimeHistogram(func(histogramOpts *prometheus.HistogramOpts) {
		histogramOpts.Buckets = c.GRPCLatencyBuckets
	})
	grpc_prometheus.EnableClientHandlingTimeHistogram(func(histogramOpts *prometheus.HistogramOpts) {
		histogramOpts.Buckets = c.GRPCLatencyBuckets
	})

	limithandler.EnableAcquireTimeHistogram(c.GRPCLatencyBuckets)
}
