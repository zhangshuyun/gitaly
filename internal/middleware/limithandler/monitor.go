package limithandler

import (
	"context"
	"strings"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/prometheus/client_golang/prometheus"
)

const acquireDurationLogThreshold = 10 * time.Millisecond

// ConcurrencyMonitor allows the concurrency monitor to be observed
type ConcurrencyMonitor interface {
	Queued(ctx context.Context)
	Dequeued(ctx context.Context)
	Enter(ctx context.Context, acquireTime time.Duration)
	Exit(ctx context.Context)
}

type nullConcurrencyMonitor struct{}

func (c *nullConcurrencyMonitor) Queued(ctx context.Context)                           {}
func (c *nullConcurrencyMonitor) Dequeued(ctx context.Context)                         {}
func (c *nullConcurrencyMonitor) Enter(ctx context.Context, acquireTime time.Duration) {}
func (c *nullConcurrencyMonitor) Exit(ctx context.Context)                             {}

type promMonitor struct {
	queuedGauge     prometheus.Gauge
	inprogressGauge prometheus.Gauge
	histogram       prometheus.Observer
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

func splitMethodName(fullMethodName string) (string, string) {
	fullMethodName = strings.TrimPrefix(fullMethodName, "/") // remove leading slash
	if i := strings.Index(fullMethodName, "/"); i >= 0 {
		return fullMethodName[:i], fullMethodName[i+1:]
	}
	return "unknown", "unknown"
}
