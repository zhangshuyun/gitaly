package helper

import (
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
		panic("we should not be relying on wrapped errors!")
		return statusWrapper{err, status.New(code, err.Error())}
	}
	return err
}

func ErrInternal(err error) error { return status.Errorf(codes.Internal, "%s", err.Error()) }
func ErrInternalf(format string, a ...interface{}) error {
	return status.Errorf(codes.Internal, format, a...)
}

func ErrInvalidArgument(err error) error { return status.Errorf(codes.InvalidArgument, err.Error()) }
func ErrInvalidArgumentf(format string, a ...interface{}) error {
	return status.Errorf(codes.InvalidArgument, format, a...)
}

func ErrPreconditionFailed(err error) error {
	return status.Errorf(codes.FailedPrecondition, "%s", err.Error())
}

func ErrPreconditionFailedf(format string, a ...interface{}) error {
	return status.Errorf(codes.FailedPrecondition, format, a...)
}

func ErrNotFound(err error) error { return status.Errorf(codes.NotFound, "%s", err.Error()) }

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
