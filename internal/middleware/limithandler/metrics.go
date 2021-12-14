package limithandler

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	histogramVec       *prometheus.HistogramVec
	inprogressGaugeVec = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "gitaly",
			Subsystem: "rate_limiting",
			Name:      "in_progress",
			Help:      "Gauge of number of concurrent in-progress calls",
		},
		[]string{"system", "grpc_service", "grpc_method"},
	)

	queuedGaugeVec = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "gitaly",
			Subsystem: "rate_limiting",
			Name:      "queued",
			Help:      "Gauge of number of queued calls",
		},
		[]string{"system", "grpc_service", "grpc_method"},
	)
)

// EnableAcquireTimeHistogram enables histograms for acquisition times
func EnableAcquireTimeHistogram(buckets []float64) {
	histogramOpts := prometheus.HistogramOpts{
		Namespace: "gitaly",
		Subsystem: "rate_limiting",
		Name:      "acquiring_seconds",
		Help:      "Histogram of time calls are rate limited (in seconds)",
		Buckets:   buckets,
	}

	histogramVec = promauto.NewHistogramVec(
		histogramOpts,
		[]string{"system", "grpc_service", "grpc_method"},
	)
}
