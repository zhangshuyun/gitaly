package commandstatshandler

import (
	"context"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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

	wrapped := grpc_middleware.WrapServerStream(stream)
	wrapped.WrappedContext = ctx

	err := handler(srv, wrapped)

	return err
}

// CommandStatsMessageProducer hooks into grpc_logrus to add more fields.
//
// It replaces github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus.DefaultMessageProducer.
//
// We cannot use ctxlogrus.AddFields() as it is not concurrency safe, and
// we may be logging concurrently. Conversely, command.Stats.Fields() is
// protected by a lock and can safely be called here.
func CommandStatsMessageProducer(ctx context.Context, format string, level logrus.Level, code codes.Code, err error, fields logrus.Fields) {
	if err != nil {
		fields[logrus.ErrorKey] = err
	}
	entry := ctxlogrus.Extract(ctx).WithContext(ctx).WithFields(fields)

	// safely inject commandstats
	if stats := command.StatsFromContext(ctx); stats != nil {
		entry = entry.WithFields(stats.Fields())
	}

	entry.Logf(level, format)
}
