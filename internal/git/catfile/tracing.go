package catfile

import (
	"context"

	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
)

type trace struct {
	span    opentracing.Span
	counter *prometheus.CounterVec

	requests map[string]int
}

// startTrace starts a new tracing span and updates metrics according to how many requests have been
// done during that trace. This must be called with two contexts: first the per-RPC context, which
// is the context of the current RPC call. And then the cache context, which is the decorrelated
// context for cached catfile processes. Spans are then created for both contexts.
func startTrace(
	ctx context.Context,
	counter *prometheus.CounterVec,
	methodName string,
) (*trace, func()) {
	span, _ := opentracing.StartSpanFromContext(ctx, methodName)

	trace := &trace{
		span:    span,
		counter: counter,
		requests: map[string]int{
			"blob":   0,
			"commit": 0,
			"tree":   0,
			"tag":    0,
			"info":   0,
		},
	}

	return trace, trace.finish
}

func (t *trace) recordRequest(requestType string) {
	t.requests[requestType]++
}

func (t *trace) finish() {
	for requestType, requestCount := range t.requests {
		if requestCount == 0 {
			continue
		}

		t.span.SetTag(requestType, requestCount)
		if t.counter != nil {
			t.counter.WithLabelValues(requestType).Add(float64(requestCount))
		}
	}

	t.span.Finish()
}
