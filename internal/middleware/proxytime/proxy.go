package proxytime

import (
	"context"
	"strconv"
	"time"

	"github.com/google/uuid"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	// RequestIDKey is the key for a unique request id generated upon every rpc request that goes through praefect
	RequestIDKey = "proxy-request-id"
)

// Unary is a gRPC server-side interceptor that provides a prometheus metric for the latency praefect adds to every gitaly request.
func Unary(tracker *TrailerTracker) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		startTime := time.Now()

		requestID := uuid.New().String()

		resp, err := handler(appendToIncomingContext(ctx, metadata.Pairs(RequestIDKey, requestID)), req)

		trailer, trailerErr := tracker.RemoveTrailer(requestID)
		if trailerErr != nil {
			grpc_logrus.Extract(ctx).WithError(trailerErr).Error("error when getting trailer from tracker")
			return resp, err
		}

		gitalyTimeTrailer := trailer.Get(gitalyTimeTrailerKey)
		if len(gitalyTimeTrailer) > 0 {
			gitalyTime, gitalyTimeErr := strconv.ParseFloat(gitalyTimeTrailer[0], 64)
			if gitalyTimeErr == nil {
				praefectTime := time.Since(startTime)
				metrics.ProxyTime.Observe((float64(praefectTime) - gitalyTime) / float64(time.Second))
			}
		}

		return resp, err
	}
}

// Stream is a gRPC server-side interceptor that provides a prometheus metric for the latency praefect adds to every gitaly request.
func Stream(tracker *TrailerTracker) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		startTime := time.Now()

		requestID := uuid.New().String()

		wrapped := grpc_middleware.WrapServerStream(ss)
		wrapped.WrappedContext = appendToIncomingContext(ss.Context(), metadata.Pairs(RequestIDKey, requestID))

		err := handler(srv, wrapped)

		trailer, trailerErr := tracker.RemoveTrailer(requestID)
		if trailerErr != nil {
			grpc_logrus.Extract(ss.Context()).WithError(trailerErr).Error("error when getting trailer from tracker")
			return err
		}

		gitalyTimeTrailer := trailer.Get(gitalyTimeTrailerKey)
		if len(gitalyTimeTrailer) > 0 {
			gitalyTime, gitalyTimeErr := strconv.ParseFloat(gitalyTimeTrailer[0], 64)
			if gitalyTimeErr == nil {
				praefectTime := time.Since(startTime)
				metrics.ProxyTime.Observe((float64(praefectTime) - gitalyTime) / float64(time.Second))
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
