package catfile

import (
	"context"

	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/labkit/correlation"
)

type trace struct {
	rpcSpan   opentracing.Span
	cacheSpan opentracing.Span
	counter   *prometheus.CounterVec

	requests map[string]int
}

// startTrace starts a new tracing span and updates metrics according to how many requests have been
// done during that trace. This must be called with two contexts: first the per-RPC context, which
// is the context of the current RPC call. And then the cache context, which is the decorrelated
// context for cached catfile processes. Spans are then created for both contexts.
func startTrace(
	rpcCtx context.Context,
	cacheCtx context.Context,
	counter *prometheus.CounterVec,
	methodName string,
) (*trace, func()) {
	rpcSpan, _ := opentracing.StartSpanFromContext(rpcCtx, methodName)

	// The per-RPC and cached context will be the same in case the process for which we're
	// creating the tracing span for is not cached, and we shouldn't create the same span twice.
	var cacheSpan opentracing.Span
	if rpcCtx != cacheCtx {
		cacheSpan, _ = opentracing.StartSpanFromContext(cacheCtx, methodName, opentracing.Tag{
			Key: "correlation_id", Value: correlation.ExtractFromContext(rpcCtx),
		})
	}

	trace := &trace{
		rpcSpan:   rpcSpan,
		cacheSpan: cacheSpan,
		counter:   counter,
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

		tag := opentracing.Tag{Key: requestType, Value: requestCount}
		tag.Set(t.rpcSpan)
		if t.cacheSpan != nil {
			tag.Set(t.cacheSpan)
		}

		if t.counter != nil {
			t.counter.WithLabelValues(requestType).Add(float64(requestCount))
		}
	}

	t.rpcSpan.Finish()
	if t.cacheSpan != nil {
		t.cacheSpan.Finish()
	}
}
