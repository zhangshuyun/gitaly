package panichandler

import (
	"context"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	_ grpc.UnaryServerInterceptor  = UnaryPanicHandler
	_ grpc.StreamServerInterceptor = StreamPanicHandler
)

// PanicHandler is a handler that will be called on a grpc panic
type PanicHandler func(methodName string, error interface{})

func toPanicError(grpcMethodName string, r interface{}) error {
	return status.Errorf(codes.Internal, "panic: %v", r)
}

// UnaryPanicHandler handles request-response panics
func UnaryPanicHandler(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	defer handleCrash(info.FullMethod, func(grpcMethodName string, r interface{}) {
		err = toPanicError(grpcMethodName, r)
	})

	return handler(ctx, req)
}

// StreamPanicHandler handles stream panics
func StreamPanicHandler(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
	defer handleCrash(info.FullMethod, func(grpcMethodName string, r interface{}) {
		err = toPanicError(grpcMethodName, r)
	})

	return handler(srv, stream)
}

var additionalHandlers []PanicHandler

// InstallPanicHandler installs additional crash handles for dealing with a panic
func InstallPanicHandler(handler PanicHandler) {
	additionalHandlers = append(additionalHandlers, handler)
}

func handleCrash(grpcMethodName string, handler PanicHandler) {
	if r := recover(); r != nil {
		log.WithFields(log.Fields{
			"error":  r,
			"method": grpcMethodName,
		}).Error("grpc panic")

		handler(grpcMethodName, r)

		for _, fn := range additionalHandlers {
			fn(grpcMethodName, r)
		}
	}
}
