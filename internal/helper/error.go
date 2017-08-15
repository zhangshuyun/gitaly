package helper

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// Unimplemented is a Go error with gRPC error code 'Unimplemented'
var Unimplemented = grpc.Errorf(codes.Unimplemented, "this rpc is not implemented")

// DecorateError unless it's already a grpc error.
//  If given nil it will return nil.
func DecorateError(code codes.Code, err error) error {
	if err != nil && grpc.Code(err) == codes.Unknown {
		return grpc.Errorf(code, "%v", err)
	}
	return err
}
