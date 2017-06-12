package sentryhandler

import (
	"time"

	raven "github.com/getsentry/raven-go"
	"github.com/grpc-ecosystem/go-grpc-middleware/tags"

	"fmt"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// UnaryLogHandler handles access times and errors for unary RPC's
func UnaryLogHandler(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	start := time.Now()
	resp, err := handler(ctx, req)

	if err != nil {
		logGrpcErrorToSentry(ctx, info.FullMethod, start, err)
	}

	return resp, err
}

// StreamLogHandler handles access times and errors for stream RPC's
func StreamLogHandler(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	start := time.Now()
	err := handler(srv, stream)

	if err != nil {
		logGrpcErrorToSentry(stream.Context(), info.FullMethod, start, err)
	}

	return err
}

func stringMap(incoming map[string]interface{}) map[string]string {
	result := make(map[string]string)
	for i, v := range incoming {
		result[i] = fmt.Sprintf("%v", v)
	}
	return result
}

func logGrpcErrorToSentry(ctx context.Context, method string, start time.Time, err error) {
	grpcErrorCode := grpc.Code(err)

	if grpcErrorCode == codes.OK {
		return
	}

	tags := grpc_ctxtags.Extract(ctx)
	ravenDetails := stringMap(tags.Values())
	ravenDetails["grpc.code"] = grpcErrorCode.String()
	ravenDetails["grpc.method"] = method
	ravenDetails["system"] = "grpc"

	raven.CaptureError(err, ravenDetails, nil)
}
