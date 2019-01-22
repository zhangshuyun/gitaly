package config

import (
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/limithandler"
)

// ConfigurePrometheus uses the global configuration to configure prometheus
func ConfigurePrometheus() {
	if len(Config.Prometheus.GRPCLatencyBuckets) == 0 {
		return
	}

	log.WithField("latencies", Config.Prometheus.GRPCLatencyBuckets).Debug("grpc prometheus histograms enabled")

	grpc_prometheus.EnableHandlingTimeHistogram(func(histogramOpts *prometheus.HistogramOpts) {
		histogramOpts.Buckets = Config.Prometheus.GRPCLatencyBuckets
	})

	limithandler.EnableAcquireTimeHistogram(Config.Prometheus.GRPCLatencyBuckets)
}
