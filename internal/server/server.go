package server

import (
	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/helper/fieldextractors"
	gitalylog "gitlab.com/gitlab-org/gitaly/internal/log"
	"gitlab.com/gitlab-org/gitaly/internal/logsanitizer"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/cancelhandler"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/limithandler"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/metadatahandler"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/panichandler"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/sentryhandler"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/server/auth"
	"gitlab.com/gitlab-org/gitaly/internal/service"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func concurrencyKeyFn(ctx context.Context) string {
	tags := grpc_ctxtags.Extract(ctx)
	ctxValue := tags.Values()["grpc.request.repoPath"]
	if ctxValue == nil {
		return ""
	}

	s, ok := ctxValue.(string)
	if ok {
		return s
	}

	return ""
}

var logrusEntry *log.Entry

func init() {
	for _, l := range gitalylog.Loggers {
		urlSanitizer := logsanitizer.NewURLSanitizerHook()
		urlSanitizer.AddPossibleGrpcMethod(
			"CreateRepositoryFromURL",
			"FetchRemote",
			"UpdateRemoteMirror",
		)
		l.Hooks.Add(urlSanitizer)
	}

	// logrusEntry is used by middlewares below
	logrusEntry = log.NewEntry(gitalylog.Default)

	// grpc-go gets a custom logger; it is too chatty
	grpc_logrus.ReplaceGrpcLogger(log.NewEntry(gitalylog.GrpcGo))
}

// New returns a GRPC server with all Gitaly services and interceptors set up.
func New(rubyServer *rubyserver.Server) *grpc.Server {
	ctxTagOpts := []grpc_ctxtags.Option{
		grpc_ctxtags.WithFieldExtractorForInitialReq(fieldextractors.FieldExtractor),
	}

	lh := limithandler.New(concurrencyKeyFn)

	server := grpc.NewServer(
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			grpc_ctxtags.StreamServerInterceptor(ctxTagOpts...),
			metadatahandler.StreamInterceptor,
			grpc_prometheus.StreamServerInterceptor,
			grpc_logrus.StreamServerInterceptor(logrusEntry),
			sentryhandler.StreamLogHandler,
			cancelhandler.Stream, // Should be below LogHandler
			lh.StreamInterceptor(),
			auth.StreamServerInterceptor(),
			// Panic handler should remain last so that application panics will be
			// converted to errors and logged
			panichandler.StreamPanicHandler,
		)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			grpc_ctxtags.UnaryServerInterceptor(ctxTagOpts...),
			metadatahandler.UnaryInterceptor,
			grpc_prometheus.UnaryServerInterceptor,
			grpc_logrus.UnaryServerInterceptor(logrusEntry),
			sentryhandler.UnaryLogHandler,
			cancelhandler.Unary, // Should be below LogHandler
			lh.UnaryInterceptor(),
			auth.UnaryServerInterceptor(),
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
