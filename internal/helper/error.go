package helper

import (
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
	if err != nil && GrpcCode(err) == codes.Unknown {
		return statusWrapper{err, status.New(code, err.Error())}
	}
	return err
}

// ErrInternal wraps err with codes.Internal, unless err is already a grpc error
func ErrInternal(err error) error { return DecorateError(codes.Internal, err) }

// ErrInternalf wrapps a formatted error with codes.Internal, unless err is already a grpc error
func ErrInternalf(format string, a ...interface{}) error {
	return ErrInternal(fmt.Errorf(format, a...))
}

// ErrInvalidArgument wraps err with codes.InvalidArgument, unless err is already a grpc error
func ErrInvalidArgument(err error) error { return DecorateError(codes.InvalidArgument, err) }

// ErrInvalidArgumentf wraps a formatted error with codes.InvalidArgument, unless err is already a grpc error
func ErrInvalidArgumentf(format string, a ...interface{}) error {
	return ErrInvalidArgument(fmt.Errorf(format, a...))
}

// ErrPreconditionFailed wraps err with codes.FailedPrecondition, unless err is already a grpc error
func ErrPreconditionFailed(err error) error { return DecorateError(codes.FailedPrecondition, err) }

// ErrPreconditionFailedf wraps a formatted error with codes.FailedPrecondition, unless err is already a grpc error
func ErrPreconditionFailedf(format string, a ...interface{}) error {
	return ErrPreconditionFailed(fmt.Errorf(format, a...))
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
