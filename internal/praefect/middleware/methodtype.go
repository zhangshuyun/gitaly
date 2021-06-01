package middleware

import (
	"context"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/metrics"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/protoregistry"
	"google.golang.org/grpc"
)

// MethodTypeUnaryInterceptor returns a Unary Interceptor that records the method type of incoming RPC requests
func MethodTypeUnaryInterceptor(r *protoregistry.Registry) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		observeMethodType(r, info.FullMethod)

		res, err := handler(ctx, req)

		return res, err
	}
}

// MethodTypeStreamInterceptor returns a Stream Interceptor that records the method type of incoming RPC requests
func MethodTypeStreamInterceptor(r *protoregistry.Registry) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		observeMethodType(r, info.FullMethod)

		err := handler(srv, stream)

		return err
	}
}

func observeMethodType(registry *protoregistry.Registry, fullMethod string) {
	if registry.IsInterceptedMethod(fullMethod) {
		return
	}

	mi, err := registry.LookupMethod(fullMethod)
	if err != nil {
		logrus.WithField("full_method_name", fullMethod).WithError(err).Warn("error when looking up method info")
	}

	var opType string
	switch mi.Operation {
	case protoregistry.OpAccessor:
		opType = "accessor"
	case protoregistry.OpMutator:
		opType = "mutator"
	default:
		return
	}

	metrics.MethodTypeCounter.WithLabelValues(opType).Inc()
}
