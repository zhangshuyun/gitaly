package server

import (
	log "github.com/Sirupsen/logrus"

	"gitlab.com/gitlab-org/gitaly/internal/helper/fieldextractors"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/cancelhandler"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/objectdirhandler"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/panichandler"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/sentryhandler"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/service"

	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/grpc-ecosystem/go-grpc-prometheus"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// New returns a GRPC server with all Gitaly services and interceptors set up.
func New(rubyServer *rubyserver.Server) *grpc.Server {
	logrusEntry := log.NewEntry(log.StandardLogger())
	grpc_logrus.ReplaceGrpcLogger(logrusEntry)

	ctxTagOpts := []grpc_ctxtags.Option{
		grpc_ctxtags.WithFieldExtractor(fieldextractors.RepositoryFieldExtractor),
	}

	server := grpc.NewServer(
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			grpc_ctxtags.StreamServerInterceptor(ctxTagOpts...),
			grpc_prometheus.StreamServerInterceptor,
			grpc_logrus.StreamServerInterceptor(logrusEntry),
			sentryhandler.StreamLogHandler,
			cancelhandler.Stream, // Should be below LogHandler
			authStreamServerInterceptor(),
			// Should be the last one, or above interceptors that
			// don't replace the stream context with a derived one.
			objectdirhandler.Stream,
			// Panic handler should remain last so that application panics will be
			// converted to errors and logged
			panichandler.StreamPanicHandler,
		)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			objectdirhandler.Unary,
			grpc_ctxtags.UnaryServerInterceptor(ctxTagOpts...),
			grpc_prometheus.UnaryServerInterceptor,
			grpc_logrus.UnaryServerInterceptor(logrusEntry),
			sentryhandler.UnaryLogHandler,
			cancelhandler.Unary, // Should be below LogHandler
			authUnaryServerInterceptor(),
			// Panic handler should remain last so that application panics will be
			// converted to errors and logged
			panichandler.UnaryPanicHandler,
		)),
	)

	service.RegisterAll(server, rubyServer)
	reflection.Register(server)

	grpc_prometheus.Register(server)

	return server
}
