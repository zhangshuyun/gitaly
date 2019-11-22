package proxytime

import (
	"context"
	"strconv"
	"time"

	"github.com/google/uuid"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	// RequestIDKey is the key for a unique request id generated upon every rpc request that goes through praefect
	RequestIDKey = "proxy-request-id"
)

// Unary is a gRPC server-side interceptor that provides a prometheus metric for the latency praefect adds to every gitaly request.
func Unary(tracker TrailerTracker) func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		startTime := time.Now()

		requestID := uuid.New().String()

		resp, err := handler(appendToIncomingContext(ctx, metadata.Pairs(RequestIDKey, requestID)), req)

		gitalyTimeTrailer := tracker.RetrieveTrailer(requestID).Get("gitaly-time")
		if len(gitalyTimeTrailer) > 0 {
			gitalyTime, err := strconv.ParseFloat(gitalyTimeTrailer[0], 64)
			if err == nil {
				praefectTime := time.Since(startTime)
				metrics.ProxyTime.Observe(float64(praefectTime) - gitalyTime)
			}
		}

		return resp, err
	}
}

// Stream is a gRPC server-side interceptor that provides a prometheus metric for the latency praefect adds to every gitaly request.
func Stream(tracker TrailerTracker) func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		startTime := time.Now()

		requestID := uuid.New().String()

		wrapped := grpc_middleware.WrapServerStream(ss)
		wrapped.WrappedContext = appendToIncomingContext(ss.Context(), metadata.Pairs(RequestIDKey, requestID))

		err := handler(srv, wrapped)

		gitalyTimeTrailer := tracker.RetrieveTrailer(requestID).Get("gitaly-time")
		if len(gitalyTimeTrailer) > 0 {
			gitalyTime, err := strconv.ParseFloat(gitalyTimeTrailer[0], 64)
			if err == nil {
				praefectTime := time.Since(startTime)
				metrics.ProxyTime.Observe(float64(praefectTime) - gitalyTime)
			}
		}

		return err
	}
}

func appendToIncomingContext(ctx context.Context, md metadata.MD) context.Context {
	existingMD, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		existingMD = make(metadata.MD)
	}

	return metadata.NewIncomingContext(ctx, metadata.Join(existingMD, md))
}
