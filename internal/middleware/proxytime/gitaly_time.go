package proxytime

import (
	"context"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// gitalyTimeTrailerKey is the key name for a trailer value representing the total time a request has been in gitaly
const gitalyTimeTrailerKey = "gitaly-time"

// StreamGitalyTime is a gRPC server-side interceptor that sets a trailer with the total time spent in gitaly.
func StreamGitalyTime(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	startTime := time.Now()
	defer func() {
		ss.SetTrailer(metadata.New(map[string]string{gitalyTimeTrailerKey: strconv.FormatFloat(time.Since(startTime).Seconds(), 'f', 10, 64)}))
	}()

	return handler(srv, ss)
}

// UnaryGitalyTime is a gRPC server-side interceptor that sets a trailer with the total time spent in gitaly.
func UnaryGitalyTime(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	startTime := time.Now()
	defer func() {
		grpc.SetTrailer(ctx, metadata.Pairs(gitalyTimeTrailerKey, strconv.FormatFloat(time.Since(startTime).Seconds(), 'f', 10, 64)))
	}()

	return handler(ctx, req)
}
