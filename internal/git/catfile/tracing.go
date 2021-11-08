package catfile

import (
	"context"
	"sync"

	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
)

type trace struct {
	span    opentracing.Span
	counter *prometheus.CounterVec

	requestsLock sync.Mutex
	requests     map[string]int
}

// startTrace starts a new tracing span and updates metrics according to how many requests have been
// done during that trace. The caller must call `finish()` on the resulting after it's deemed to be
// done such that metrics get recorded correctly.
func startTrace(
	ctx context.Context,
	counter *prometheus.CounterVec,
	methodName string,
) *trace {
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

	return trace
}

func (t *trace) recordRequest(requestType string) {
	t.requestsLock.Lock()
	defer t.requestsLock.Unlock()
	t.requests[requestType]++
}

func (t *trace) finish() {
	t.requestsLock.Lock()
	defer t.requestsLock.Unlock()

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
