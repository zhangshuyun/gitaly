package errorhandler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"

	"google.golang.org/grpc"
)

func UnaryErrorHandler(errorTracker *Errors, registry *protoregistry.Registry) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			mi, lookupErr := registry.LookupMethod(info.FullMethod)
			if err != nil {
				return resp, fmt.Errorf("error when looking up method :%w %w", err, lookupErr)
			}
			switch mi.Operation {
			case protoregistry.OpAccessor:
				errorTracker.IncrReadErr()
			case protoregistry.OpMutator:
				errorTracker.IncrWriteErr()
			}
		}
		return resp, err
	}
}

func StreamErrorHandler(errorTracker *Errors, registry *protoregistry.Registry) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := handler(srv, stream)
		if err != nil {
			mi, lookupErr := registry.LookupMethod(info.FullMethod)
			if err != nil {
				return fmt.Errorf("error when looking up method :%w %w", err, lookupErr)
			}
			switch mi.Operation {
			case protoregistry.OpAccessor:
				errorTracker.IncrReadErr()
			case protoregistry.OpMutator:
				errorTracker.IncrWriteErr()
			}
		}

		return err
	}
}

type Errors struct {
	m                       sync.RWMutex
	readErrors, writeErrors int64
}

func (e *Errors) IncrReadErr() {
	e.m.Lock()
	defer e.m.Unlock()

	e.readErrors++
}

func (e *Errors) IncrWriteErr() {
	e.m.Lock()
	defer e.m.Unlock()

	e.writeErrors++
}

func (e *Errors) ReadErrs() int64 {
	e.m.RLock()
	defer e.m.RUnlock()

	return e.readErrors
}

func (e *Errors) WriteErrs() int64 {
	e.m.RLock()
	defer e.m.RUnlock()

	return e.writeErrors
}

func (e *Errors) PeriodicallyClear() {
	ticker := time.NewTicker(10 * time.Second)
	for {
		<-ticker.C
	}
}

func (e *Errors) clear() {
	e.m.Lock()
	defer e.m.Unlock()
	e.readErrors = 0
	e.writeErrors = 0
}
