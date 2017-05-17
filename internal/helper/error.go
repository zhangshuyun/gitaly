package helper

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// DecorateError unless it's already a grpc error.
//  If given nil it will return nil.
func DecorateError(code codes.Code, err error) error {
	if err != nil && grpc.Code(err) == codes.Unknown {
		return grpc.Errorf(code, "%v", err)
	}
	return err
}
