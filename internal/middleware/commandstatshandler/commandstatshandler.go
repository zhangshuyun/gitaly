package commandstatshandler

import (
	"context"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v13/internal/command"
	"google.golang.org/grpc"
)

// UnaryInterceptor returns a Unary Interceptor
func UnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	ctx = command.InitContextStats(ctx)

	res, err := handler(ctx, req)

	stats := command.StatsFromContext(ctx)
	ctxlogrus.AddFields(ctx, stats.Fields())

	return res, err
}

// StreamInterceptor returns a Stream Interceptor
func StreamInterceptor(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := stream.Context()
	ctx = command.InitContextStats(ctx)

	wrapped := grpc_middleware.WrapServerStream(stream)
	wrapped.WrappedContext = ctx

	err := handler(srv, wrapped)

	stats := command.StatsFromContext(ctx)
	ctxlogrus.AddFields(ctx, stats.Fields())

	return err
}
