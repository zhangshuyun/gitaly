package sentryhandler

import (
	"fmt"
	"strings"
	"time"

	raven "github.com/getsentry/raven-go"
	"github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var ignoredCodes = []codes.Code{
	// OK means there was no error
	codes.OK,
	// Canceled and DeadlineExceeded indicate clients that disappeared or lost interest
	codes.Canceled,
	codes.DeadlineExceeded,
	// We use FailedPrecondition to signal error conditions that are 'normal'
	codes.FailedPrecondition,
}

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

func methodToCulprit(methodName string) string {
	methodName = strings.TrimPrefix(methodName, "/gitaly.")
	methodName = strings.Replace(methodName, "/", "::", 1)
	return methodName
}

func logErrorToSentry(err error) (code codes.Code, bypass bool) {
	code = helper.GrpcCode(err)

	for _, ignoredCode := range ignoredCodes {
		if code == ignoredCode {
			return code, true
		}
	}

	return code, false
}

func generateRavenPacket(ctx context.Context, method string, start time.Time, err error) (*raven.Packet, map[string]string) {
	grpcErrorCode, bypass := logErrorToSentry(err)
	if bypass {
		return nil, nil
	}

	tags := grpc_ctxtags.Extract(ctx)
	ravenDetails := stringMap(tags.Values())

	ravenDetails["grpc.code"] = grpcErrorCode.String()
	ravenDetails["grpc.method"] = method
	ravenDetails["grpc.time_ms"] = fmt.Sprintf("%.0f", time.Since(start).Seconds()*1000)
	ravenDetails["system"] = "grpc"

	// Skip the stacktrace as it's not helpful in this context
	packet := raven.NewPacket(err.Error(), raven.NewException(err, nil))
	grpcMethod := methodToCulprit(method)

	// Details on fingerprinting
	// https://docs.sentry.io/learn/rollups/#customize-grouping-with-fingerprints
	packet.Fingerprint = []string{"grpc", grpcMethod, grpcErrorCode.String()}
	packet.Culprit = grpcMethod
	return packet, ravenDetails
}

func logGrpcErrorToSentry(ctx context.Context, method string, start time.Time, err error) {
	packet, tags := generateRavenPacket(ctx, method, start, err)
	if packet == nil {
		return
	}

	raven.Capture(packet, tags)
}
