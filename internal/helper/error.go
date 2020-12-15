package helper

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Unimplemented is a Go error with gRPC error code 'Unimplemented'
var Unimplemented = status.Errorf(codes.Unimplemented, "this rpc is not implemented")

// DecorateError unless it's already a grpc error.
//  If given nil it will return nil.
func DecorateError(code codes.Code, err error) error {
	if err != nil && GrpcCode(err) == codes.Unknown {
		return status.Errorf(code, "%v", err)
	}
	return err
}

// ErrInternal wraps err with codes.Internal, unless err is already a grpc error
func ErrInternal(err error) error { return DecorateError(codes.Internal, err) }

// ErrInternalf wrapps a formatted error with codes.Internal, unless err is already a grpc error
func ErrInternalf(format string, a ...interface{}) error {
	return DecorateError(codes.Internal, fmt.Errorf(format, a...))
}

// ErrInvalidArgument wraps err with codes.InvalidArgument, unless err is already a grpc error
func ErrInvalidArgument(err error) error { return DecorateError(codes.InvalidArgument, err) }

// ErrInvalidArgumentf wraps a formatted error with codes.InvalidArgument, unless err is already a grpc error
func ErrInvalidArgumentf(format string, a ...interface{}) error {
	return DecorateError(codes.InvalidArgument, fmt.Errorf(format, a...))
}

// ErrPreconditionFailed wraps err with codes.FailedPrecondition, unless err is already a grpc error
func ErrPreconditionFailed(err error) error { return DecorateError(codes.FailedPrecondition, err) }

// ErrPreconditionFailedf wraps a formatted error with codes.FailedPrecondition, unless err is already a grpc error
func ErrPreconditionFailedf(format string, a ...interface{}) error {
	return DecorateError(codes.FailedPrecondition, fmt.Errorf(format, a...))
}

// ErrNotFound wraps error with codes.NotFound, unless err is already a grpc error
func ErrNotFound(err error) error { return DecorateError(codes.NotFound, err) }

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
