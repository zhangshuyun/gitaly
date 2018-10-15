package limithandler

import (
	"strings"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	prom "github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"
)

const acquireDurationLogThreshold = 10 * time.Millisecond

var (
	histogramEnabled   = false
	histogramVec       *prom.HistogramVec
	inprogressGaugeVec = prom.NewGaugeVec(
		prom.GaugeOpts{
			Namespace: "gitaly",
			Subsystem: "rate_limiting",
			Name:      "in_progress",
			Help:      "Gauge of number of number of concurrent invocations currently in progress for this endpoint",
		},
		[]string{"system", "grpc_service", "grpc_method"},
	)

	queuedGaugeVec = prom.NewGaugeVec(
		prom.GaugeOpts{
			Namespace: "gitaly",
			Subsystem: "rate_limiting",
			Name:      "queued",
			Help:      "Gauge of number of number of invocations currently queued for this endpoint",
		},
		[]string{"system", "grpc_service", "grpc_method"},
	)
)

type promMonitor struct {
	queuedGauge     prom.Gauge
	inprogressGauge prom.Gauge
	histogram       prom.Histogram
}

func init() {
	prom.MustRegister(inprogressGaugeVec, queuedGaugeVec)
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
	histogramEnabled = true
	histogramOpts := prom.HistogramOpts{
		Namespace: "gitaly",
		Subsystem: "rate_limiting",
		Name:      "acquiring_seconds",
		Help:      "Histogram of lock acquisition latency (seconds) for endpoint rate limiting",
		Buckets:   buckets,
	}

	histogramVec = prom.NewHistogramVec(
		histogramOpts,
		[]string{"system", "grpc_service", "grpc_method"},
	)

	prom.Register(histogramVec)
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
		logger := grpc_logrus.Extract(ctx)
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

	var histogram prom.Histogram
	if histogramVec != nil {
		histogram = histogramVec.WithLabelValues(system, serviceName, methodName)
	}

	return &promMonitor{queuedGauge, inprogressGauge, histogram}
}
