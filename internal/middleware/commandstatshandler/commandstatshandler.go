package commandstatshandler

import (
	"context"

	grpcmw "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"google.golang.org/grpc"
)

// UnaryInterceptor returns a Unary Interceptor
func UnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	ctx = command.InitContextStats(ctx)

	res, err := handler(ctx, req)

	return res, err
}

// StreamInterceptor returns a Stream Interceptor
func StreamInterceptor(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := stream.Context()
	ctx = command.InitContextStats(ctx)

	wrapped := grpcmw.WrapServerStream(stream)
	wrapped.WrappedContext = ctx

	err := handler(srv, wrapped)

	return err
}

// FieldsProducer extracts stats info from the context and returns it as a logging fields.
func FieldsProducer(ctx context.Context) logrus.Fields {
	if stats := command.StatsFromContext(ctx); stats != nil {
		return stats.Fields()
	}
	return nil
}
