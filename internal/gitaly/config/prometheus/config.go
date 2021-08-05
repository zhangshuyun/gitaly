package prometheus

import (
	"time"

	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/middleware/limithandler"
)

// Config contains additional configuration data for prometheus
type Config struct {
	// ScrapeTimeout is the allowed duration of a Prometheus scrape before timing out.
	ScrapeTimeout time.Duration `toml:"scrape_timeout"`
	// GRPCLatencyBuckets configures the histogram buckets used for gRPC
	// latency measurements.
	GRPCLatencyBuckets []float64 `toml:"grpc_latency_buckets"`
}

// DefaultConfig returns a new config with default values set.
func DefaultConfig() Config {
	return Config{
		ScrapeTimeout:      10 * time.Second,
		GRPCLatencyBuckets: []float64{0.001, 0.005, 0.025, 0.1, 0.5, 1.0, 10.0, 30.0, 60.0, 300.0, 1500.0},
	}
}

// Configure configures latency buckets for prometheus timing histograms
func (c *Config) Configure() {
	if len(c.GRPCLatencyBuckets) == 0 {
		return
	}

	log.WithField("latencies", c.GRPCLatencyBuckets).Info("grpc prometheus histograms enabled")

	grpcprometheus.EnableHandlingTimeHistogram(func(histogramOpts *prometheus.HistogramOpts) {
		histogramOpts.Buckets = c.GRPCLatencyBuckets
	})
	grpcprometheus.EnableClientHandlingTimeHistogram(func(histogramOpts *prometheus.HistogramOpts) {
		histogramOpts.Buckets = c.GRPCLatencyBuckets
	})

	limithandler.EnableAcquireTimeHistogram(c.GRPCLatencyBuckets)
}
