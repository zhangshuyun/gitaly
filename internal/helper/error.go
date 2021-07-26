package helper

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type statusWrapper struct {
	error
	status *status.Status
}

func (sw statusWrapper) GRPCStatus() *status.Status {
	return sw.status
}

func (sw statusWrapper) Unwrap() error {
	return sw.error
}

// ErrCanceled wraps err with codes.Canceled, unless err is already a gRPC error.
func ErrCanceled(err error) error { return wrapError(codes.Canceled, err) }

// ErrInternal wraps err with codes.Internal, unless err is already a gRPC error.
func ErrInternal(err error) error { return wrapError(codes.Internal, err) }

// ErrInvalidArgument wraps err with codes.InvalidArgument, unless err is already a gRPC error.
func ErrInvalidArgument(err error) error { return wrapError(codes.InvalidArgument, err) }

// ErrNotFound wraps error with codes.NotFound, unless err is already a gRPC error.
func ErrNotFound(err error) error { return wrapError(codes.NotFound, err) }

// ErrFailedPrecondition wraps err with codes.FailedPrecondition, unless err is already a gRPC
// error.
func ErrFailedPrecondition(err error) error { return wrapError(codes.FailedPrecondition, err) }

// ErrUnavailable wraps err with codes.Unavailable, unless err is already a gRPC error.
func ErrUnavailable(err error) error { return wrapError(codes.Unavailable, err) }

// wrapError wraps the given error with the error code unless it's already a gRPC error. If given
// nil it will return nil.
func wrapError(code codes.Code, err error) error {
	if GrpcCode(err) == codes.Unknown {
		return statusWrapper{err, status.New(code, err.Error())}
	}
	return err
}

// ErrInternalf wraps a formatted error with codes.Internal, unless the formatted error is a
// wrapped gRPC error.
func ErrInternalf(format string, a ...interface{}) error {
	return formatError(codes.Internal, format, a...)
}

// ErrInvalidArgumentf wraps a formatted error with codes.InvalidArgument, unless the formatted
// error is a wrapped gRPC error.
func ErrInvalidArgumentf(format string, a ...interface{}) error {
	return formatError(codes.InvalidArgument, format, a...)
}

// ErrNotFoundf wraps a formatted error with codes.NotFound, unless the
// formatted error is a wrapped gRPC error.
func ErrNotFoundf(format string, a ...interface{}) error {
	return formatError(codes.NotFound, format, a...)
}

// ErrFailedPreconditionf wraps a formatted error with codes.FailedPrecondition, unless the
// formatted error is a wrapped gRPC error.
func ErrFailedPreconditionf(format string, a ...interface{}) error {
	return formatError(codes.FailedPrecondition, format, a...)
}

// ErrUnavailablef wraps a formatted error with codes.Unavailable, unless the
// formatted error is a wrapped gRPC error.
func ErrUnavailablef(format string, a ...interface{}) error {
	return formatError(codes.Unavailable, format, a...)
}

// formatError will create a new error from the given format string. If the error string contains a
// %w verb and its corresponding error has a gRPC error code, then the returned error will keep this
// gRPC error code instead of using the one provided as an argument.
func formatError(code codes.Code, format string, a ...interface{}) error {
	err := fmt.Errorf(format, a...)

	nestedCode := GrpcCode(errors.Unwrap(err))
	if nestedCode != codes.OK && nestedCode != codes.Unknown {
		code = nestedCode
	}

	return statusWrapper{err, status.New(code, err.Error())}
}

// GrpcCode emulates the old grpc.Code function: it translates errors into codes.Code values.
func GrpcCode(err error) codes.Code {
	if err == nil {
		return codes.OK
	}

	st, ok := status.FromError(err)
	if !ok {
		return codes.Unknown
	}

	return st.Code()
}
