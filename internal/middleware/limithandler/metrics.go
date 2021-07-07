package limithandler

import (
	"context"
	"strings"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const acquireDurationLogThreshold = 10 * time.Millisecond

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

type promMonitor struct {
	queuedGauge     prometheus.Gauge
	inprogressGauge prometheus.Gauge
	histogram       prometheus.Observer
}

func splitMethodName(fullMethodName string) (string, string) {
	fullMethodName = strings.TrimPrefix(fullMethodName, "/") // remove leading slash
	if i := strings.Index(fullMethodName, "/"); i >= 0 {
		return fullMethodName[:i], fullMethodName[i+1:]
	}
	return "unknown", "unknown"
}

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

func (c *promMonitor) Queued(ctx context.Context) {
	c.queuedGauge.Inc()
}

func (c *promMonitor) Dequeued(ctx context.Context) {
	c.queuedGauge.Dec()
}

func (c *promMonitor) Enter(ctx context.Context, acquireTime time.Duration) {
	c.inprogressGauge.Inc()

	if acquireTime > acquireDurationLogThreshold {
		logger := ctxlogrus.Extract(ctx)
		logger.WithField("acquire_ms", acquireTime.Seconds()*1000).Info("Rate limit acquire wait")
	}

	if c.histogram != nil {
		c.histogram.Observe(acquireTime.Seconds())
	}
}

func (c *promMonitor) Exit(ctx context.Context) {
	c.inprogressGauge.Dec()
}

// NewPromMonitor creates a new ConcurrencyMonitor that tracks limiter
// activity in Prometheus.
func NewPromMonitor(system string, fullMethod string) ConcurrencyMonitor {
	serviceName, methodName := splitMethodName(fullMethod)

	queuedGauge := queuedGaugeVec.WithLabelValues(system, serviceName, methodName)
	inprogressGauge := inprogressGaugeVec.WithLabelValues(system, serviceName, methodName)

	var histogram prometheus.Observer
	if histogramVec != nil {
		histogram = histogramVec.WithLabelValues(system, serviceName, methodName)
	}

	return &promMonitor{queuedGauge, inprogressGauge, histogram}
}
