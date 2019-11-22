package proxytime

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// GitalyTimeTrailerKey is the key name for a trailer value representing the total time a request has been in gitaly
const GitalyTimeTrailerKey = "gitaly-time"

// StreamGitalyTime is a gRPC server-side interceptor that sets a trailer with the total time spent in gitaly.
func StreamGitalyTime(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	startTime := time.Now()
	defer func() {
		ss.SetTrailer(metadata.New(map[string]string{GitalyTimeTrailerKey: fmt.Sprintf("%d", int64(time.Since(startTime)))}))
	}()

	return handler(srv, ss)
}

// UnaryGitalyTime is a gRPC server-side interceptor that sets a trailer with the total time spent in gitaly.
func UnaryGitalyTime(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	startTime := time.Now()
	defer func() {
		grpc.SetTrailer(ctx, metadata.Pairs(GitalyTimeTrailerKey, fmt.Sprintf("%d", int64(time.Since(startTime)))))
	}()

	return handler(ctx, req)
}
