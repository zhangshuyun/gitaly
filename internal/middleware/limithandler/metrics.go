package limithandler

import (
	"context"
	"strings"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/prometheus/client_golang/prometheus"
)

const acquireDurationLogThreshold = 10 * time.Millisecond

var (
	histogramVec       *prometheus.HistogramVec
	inprogressGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "gitaly",
			Subsystem: "rate_limiting",
			Name:      "in_progress",
			Help:      "Gauge of number of number of concurrent invocations currently in progress for this endpoint",
		},
		[]string{"system", "grpc_service", "grpc_method"},
	)

	queuedGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "gitaly",
			Subsystem: "rate_limiting",
			Name:      "queued",
			Help:      "Gauge of number of number of invocations currently queued for this endpoint",
		},
		[]string{"system", "grpc_service", "grpc_method"},
	)
)

type promMonitor struct {
	queuedGauge     prometheus.Gauge
	inprogressGauge prometheus.Gauge
	histogram       prometheus.Observer
}

func init() {
	prometheus.MustRegister(inprogressGaugeVec, queuedGaugeVec)
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
		Help:      "Histogram of lock acquisition latency (seconds) for endpoint rate limiting",
		Buckets:   buckets,
	}

	histogramVec = prometheus.NewHistogramVec(
		histogramOpts,
		[]string{"system", "grpc_service", "grpc_method"},
	)

	prometheus.Register(histogramVec)
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

	queuedGauge := queuedGaugeVec.WithLabelValues(serviceName, methodName, system)
	inprogressGauge := inprogressGaugeVec.WithLabelValues(serviceName, methodName, system)

	var histogram prometheus.Observer
	if histogramVec != nil {
		histogram = histogramVec.WithLabelValues(system, serviceName, methodName)
	}

	return &promMonitor{queuedGauge, inprogressGauge, histogram}
}
