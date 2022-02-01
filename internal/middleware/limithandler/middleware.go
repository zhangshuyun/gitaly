package limithandler

import (
	"context"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	grpcmwtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"google.golang.org/grpc"
)

// GetLockKey function defines the lock key of an RPC invocation based on its context
type GetLockKey func(context.Context) string

// LimitConcurrencyByRepo implements GetLockKey by using the repository path as lock.
func LimitConcurrencyByRepo(ctx context.Context) string {
	tags := grpcmwtags.Extract(ctx)
	ctxValue := tags.Values()["grpc.request.repoPath"]
	if ctxValue == nil {
		return ""
	}

	s, ok := ctxValue.(string)
	if ok {
		return s
	}

	return ""
}

// LimiterMiddleware contains rate limiter state
type LimiterMiddleware struct {
	methodLimiters map[string]*ConcurrencyLimiter
	getLockKey     GetLockKey

	acquiringSecondsMetric *prometheus.HistogramVec
	inProgressMetric       *prometheus.GaugeVec
	queuedMetric           *prometheus.GaugeVec
}

// New creates a new rate limiter
func New(cfg config.Cfg, getLockKey GetLockKey) *LimiterMiddleware {
	middleware := &LimiterMiddleware{
		getLockKey: getLockKey,

		acquiringSecondsMetric: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "gitaly",
				Subsystem: "rate_limiting",
				Name:      "acquiring_seconds",
				Help:      "Histogram of time calls are rate limited (in seconds)",
				Buckets:   cfg.Prometheus.GRPCLatencyBuckets,
			},
			[]string{"system", "grpc_service", "grpc_method"},
		),
		inProgressMetric: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "gitaly",
				Subsystem: "rate_limiting",
				Name:      "in_progress",
				Help:      "Gauge of number of concurrent in-progress calls",
			},
			[]string{"system", "grpc_service", "grpc_method"},
		),
		queuedMetric: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "gitaly",
				Subsystem: "rate_limiting",
				Name:      "queued",
				Help:      "Gauge of number of queued calls",
			},
			[]string{"system", "grpc_service", "grpc_method"},
		),
	}
	middleware.methodLimiters = createLimiterConfig(middleware, cfg)
	return middleware
}

// Describe is used to describe Prometheus metrics.
func (c *LimiterMiddleware) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, descs)
}

// Collect is used to collect Prometheus metrics.
func (c *LimiterMiddleware) Collect(metrics chan<- prometheus.Metric) {
	c.acquiringSecondsMetric.Collect(metrics)
	c.inProgressMetric.Collect(metrics)
	c.queuedMetric.Collect(metrics)
}

// UnaryInterceptor returns a Unary Interceptor
func (c *LimiterMiddleware) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		lockKey := c.getLockKey(ctx)
		if lockKey == "" {
			return handler(ctx, req)
		}

		limiter := c.methodLimiters[info.FullMethod]
		if limiter == nil {
			// No concurrency limiting
			return handler(ctx, req)
		}

		return limiter.Limit(ctx, lockKey, func() (interface{}, error) {
			return handler(ctx, req)
		})
	}
}

// StreamInterceptor returns a Stream Interceptor
func (c *LimiterMiddleware) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		wrapper := &wrappedStream{stream, info, c, true}
		return handler(srv, wrapper)
	}
}

func createLimiterConfig(middleware *LimiterMiddleware, cfg config.Cfg) map[string]*ConcurrencyLimiter {
	result := make(map[string]*ConcurrencyLimiter)

	newTickerFunc := func() helper.Ticker {
		return helper.NewManualTicker()
	}

	for _, limit := range cfg.Concurrency {
		if limit.MaxQueueWait > 0 {
			limit := limit
			newTickerFunc = func() helper.Ticker {
				return helper.NewTimerTicker(limit.MaxQueueWait.Duration())
			}
		}

		result[limit.RPC] = NewLimiter(
			limit.MaxPerRepo,
			limit.MaxQueueSize,
			newTickerFunc,
			newPromMonitor(middleware, "gitaly", limit.RPC),
		)
	}

	// Set default for ReplicateRepository.
	replicateRepositoryFullMethod := "/gitaly.RepositoryService/ReplicateRepository"
	if _, ok := result[replicateRepositoryFullMethod]; !ok {
		result[replicateRepositoryFullMethod] = NewLimiter(
			1,
			0,
			func() helper.Ticker {
				return helper.NewManualTicker()
			},
			newPromMonitor(middleware, "gitaly", replicateRepositoryFullMethod))
	}

	return result
}

type wrappedStream struct {
	grpc.ServerStream
	info              *grpc.StreamServerInfo
	limiterMiddleware *LimiterMiddleware
	initial           bool
}

func (w *wrappedStream) RecvMsg(m interface{}) error {
	if err := w.ServerStream.RecvMsg(m); err != nil {
		return err
	}

	// Only perform limiting on the first request of a stream
	if !w.initial {
		return nil
	}

	w.initial = false

	ctx := w.Context()

	lockKey := w.limiterMiddleware.getLockKey(ctx)
	if lockKey == "" {
		return nil
	}

	limiter := w.limiterMiddleware.methodLimiters[w.info.FullMethod]
	if limiter == nil {
		// No concurrency limiting
		return nil
	}

	ready := make(chan struct{})
	go func() {
		if _, err := limiter.Limit(ctx, lockKey, func() (interface{}, error) {
			close(ready)
			<-ctx.Done()
			return nil, nil
		}); err != nil {
			ctxlogrus.Extract(ctx).WithError(err).Error("rate limiting streaming request")
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ready:
		// It's our turn!
		return nil
	}
}
