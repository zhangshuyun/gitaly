package sentryhandler

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	sentry "github.com/getsentry/sentry-go"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

const (
	skipSubmission = "sentry.skip"
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

func logErrorToSentry(ctx context.Context, err error) (code codes.Code, bypass bool) {
	code = helper.GrpcCode(err)

	for _, ignoredCode := range ignoredCodes {
		if code == ignoredCode {
			return code, true
		}
	}

	tags := grpc_ctxtags.Extract(ctx)
	if tags.Has(skipSubmission) {
		return code, true
	}

	return code, false
}

func generateSentryEvent(ctx context.Context, method string, start time.Time, err error) *sentry.Event {
	grpcErrorCode, bypass := logErrorToSentry(ctx, err)
	if bypass {
		return nil
	}

	tags := grpc_ctxtags.Extract(ctx)
	event := sentry.NewEvent()

	for k, v := range stringMap(tags.Values()) {
		event.Tags[k] = v
	}

	for k, v := range map[string]string{
		"grpc.code":    grpcErrorCode.String(),
		"grpc.method":  method,
		"grpc.time_ms": fmt.Sprintf("%.0f", time.Since(start).Seconds()*1000),
		"system":       "grpc",
	} {
		event.Tags[k] = v
	}

	event.Message = err.Error()

	// Skip the stacktrace as it's not helpful in this context
	event.Exception = append(event.Exception, newException(err, nil))

	grpcMethod := methodToCulprit(method)

	// Details on fingerprinting
	// https://docs.sentry.io/learn/rollups/#customize-grouping-with-fingerprints
	event.Fingerprint = []string{"grpc", grpcMethod, grpcErrorCode.String()}
	event.Transaction = grpcMethod

	return event
}

func logGrpcErrorToSentry(ctx context.Context, method string, start time.Time, err error) {
	event := generateSentryEvent(ctx, method, start, err)
	if event == nil {
		return
	}

	sentry.CaptureEvent(event)
}

var errorMsgPattern = regexp.MustCompile(`\A(\w+): (.+)\z`)

// newException constructs an Exception using provided Error and Stacktrace
func newException(err error, stacktrace *sentry.Stacktrace) sentry.Exception {
	msg := err.Error()
	ex := sentry.Exception{
		Stacktrace: stacktrace,
		Value:      msg,
		Type:       reflect.TypeOf(err).String(),
	}
	if m := errorMsgPattern.FindStringSubmatch(msg); m != nil {
		ex.Module, ex.Value = m[1], m[2]
	}
	return ex
}

// MarkToSkip propagate context with a special tag that signals to sentry handler that the error must not be reported.
func MarkToSkip(ctx context.Context) {
	tags := grpc_ctxtags.Extract(ctx)
	tags.Set(skipSubmission, struct{}{})
}
