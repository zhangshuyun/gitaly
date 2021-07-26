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

// DecorateError unless it's already a grpc error.
//  If given nil it will return nil.
func DecorateError(code codes.Code, err error) error {
	if GrpcCode(err) == codes.Unknown {
		return statusWrapper{err, status.New(code, err.Error())}
	}
	return err
}

// ErrInternal wraps err with codes.Internal, unless err is already a grpc error
func ErrInternal(err error) error { return DecorateError(codes.Internal, err) }

// ErrInvalidArgument wraps err with codes.InvalidArgument, unless err is already a grpc error
func ErrInvalidArgument(err error) error { return DecorateError(codes.InvalidArgument, err) }

// ErrPreconditionFailed wraps err with codes.FailedPrecondition, unless err is already a grpc error
func ErrPreconditionFailed(err error) error { return DecorateError(codes.FailedPrecondition, err) }

// ErrNotFound wraps error with codes.NotFound, unless err is already a grpc error
func ErrNotFound(err error) error { return DecorateError(codes.NotFound, err) }

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

// ErrPreconditionFailedf wraps a formatted error with codes.FailedPrecondition, unless the
// formatted error is a wrapped gRPC error.
func ErrPreconditionFailedf(format string, a ...interface{}) error {
	return formatError(codes.FailedPrecondition, format, a...)
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
