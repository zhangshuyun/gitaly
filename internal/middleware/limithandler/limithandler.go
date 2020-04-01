package limithandler

import (
	"context"

	"google.golang.org/grpc"
)

// GetLockKey function defines the lock key of an RPC invocation based on its context
type GetLockKey func(context.Context) string

// LimiterMiddleware contains rate limiter state
type LimiterMiddleware struct {
	methodLimiters map[string]*ConcurrencyLimiter
	getLockKey     GetLockKey
}

type wrappedStream struct {
	grpc.ServerStream
	info              *grpc.StreamServerInfo
	limiterMiddleware *LimiterMiddleware
	initial           bool
}

var maxConcurrencyPerRepoPerRPC map[string]int

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
	go limiter.Limit(ctx, lockKey, func() (interface{}, error) {
		close(ready)
		<-ctx.Done()
		return nil, nil
	})

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ready:
		// It's our turn!
		return nil
	}
}

// New creates a new rate limiter
func New(getLockKey GetLockKey) LimiterMiddleware {
	return LimiterMiddleware{
		methodLimiters: createLimiterConfig(),
		getLockKey:     getLockKey,
	}
}

func createLimiterConfig() map[string]*ConcurrencyLimiter {
	result := make(map[string]*ConcurrencyLimiter)

	for fullMethodName, max := range maxConcurrencyPerRepoPerRPC {
		result[fullMethodName] = NewLimiter(max, NewPromMonitor("gitaly", fullMethodName))
	}

	for fullMethodName, _ := range idempotentRPCs {
		result[fullMethodName] = NewIdempotentLimiter(NewPromMonitor("gitaly", fullMethodName))
	}

	return result
}

// SetMaxRepoConcurrency Configures the max concurrency per repo per RPC
func SetMaxRepoConcurrency(config map[string]int) {
	maxConcurrencyPerRepoPerRPC = config
}

var idempotentRPCs = map[string]struct{}{
	"/gitaly.RepositoryService/ReplicateRepository": {},
	"/gitaly.RepositoryService/OptimizeRepository":  {},
}
