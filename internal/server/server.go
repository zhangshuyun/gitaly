package server

import (
	"context"
	"crypto/tls"
	"os"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	log "github.com/sirupsen/logrus"
	diskcache "gitlab.com/gitlab-org/gitaly/internal/cache"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper/fieldextractors"
	gitalylog "gitlab.com/gitlab-org/gitaly/internal/log"
	"gitlab.com/gitlab-org/gitaly/internal/logsanitizer"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/cache"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/cancelhandler"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/limithandler"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/metadatahandler"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/notifier"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/panichandler"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/sentryhandler"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/server/auth"
	"gitlab.com/gitlab-org/gitaly/internal/service"
	grpccorrelation "gitlab.com/gitlab-org/labkit/correlation/grpc"
	grpctracing "gitlab.com/gitlab-org/labkit/tracing/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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
	logrusEntry = gitalylog.Default()

	// grpc-go gets a custom logger; it is too chatty
	grpc_logrus.ReplaceGrpcLogger(gitalylog.GrpcGo())
}

// createNewServer returns a GRPC server with all Gitaly services and interceptors set up.
// allows for specifying secure = true to enable tls credentials
func createNewServer(rubyServer *rubyserver.Server, secure bool) *grpc.Server {
	ctxTagOpts := []grpc_ctxtags.Option{
		grpc_ctxtags.WithFieldExtractorForInitialReq(fieldextractors.FieldExtractor),
	}

	lh := limithandler.New(concurrencyKeyFn)
	gitlabRails := config.Config.GitlabRails

	opts := []grpc.ServerOption{
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			grpc_ctxtags.StreamServerInterceptor(ctxTagOpts...),
			grpccorrelation.StreamServerCorrelationInterceptor(), // Must be above the metadata handler
			metadatahandler.StreamInterceptor,
			grpc_prometheus.StreamServerInterceptor,
			grpc_logrus.StreamServerInterceptor(logrusEntry),
			sentryhandler.StreamLogHandler,
			cancelhandler.Stream, // Should be below LogHandler
			lh.StreamInterceptor(),
			auth.StreamServerInterceptor(config.Config.Auth),
			grpctracing.StreamServerTracingInterceptor(),
			cache.StreamInvalidator(diskcache.LeaseKeyer{}, protoregistry.GitalyProtoPreregistered),
			notifier.StreamNotifier(gitlabRails, protoregistry.GitalyProtoPreregistered),
			// Panic handler should remain last so that application panics will be
			// converted to errors and logged
			panichandler.StreamPanicHandler,
		)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			grpc_ctxtags.UnaryServerInterceptor(ctxTagOpts...),
			grpccorrelation.UnaryServerCorrelationInterceptor(), // Must be above the metadata handler
			metadatahandler.UnaryInterceptor,
			grpc_prometheus.UnaryServerInterceptor,
			grpc_logrus.UnaryServerInterceptor(logrusEntry),
			sentryhandler.UnaryLogHandler,
			cancelhandler.Unary, // Should be below LogHandler
			lh.UnaryInterceptor(),
			auth.UnaryServerInterceptor(config.Config.Auth),
			grpctracing.UnaryServerTracingInterceptor(),
			cache.UnaryInvalidator(diskcache.LeaseKeyer{}, protoregistry.GitalyProtoPreregistered),
			notifier.UnaryNotifier(gitlabRails, protoregistry.GitalyProtoPreregistered),
			// Panic handler should remain last so that application panics will be
			// converted to errors and logged
			panichandler.UnaryPanicHandler,
		)),
	}

	// If tls config is specified attempt to extract tls options and use it
	// as a grpc.ServerOption
	if secure {
		cert, err := tls.LoadX509KeyPair(config.Config.TLS.CertPath, config.Config.TLS.KeyPath)
		if err != nil {
			log.Fatal(err)
		}
		opts = append(opts, grpc.Creds(credentials.NewServerTLSFromCert(&cert)))
	}

	server := grpc.NewServer(opts...)

	service.RegisterAll(server, rubyServer)
	reflection.Register(server)

	grpc_prometheus.Register(server)

	return server
}

// NewInsecure returns a GRPC server with all Gitaly services and interceptors set up.
func NewInsecure(rubyServer *rubyserver.Server) *grpc.Server {
	return createNewServer(rubyServer, false)
}

// NewSecure returns a GRPC server enabling TLS credentials
func NewSecure(rubyServer *rubyserver.Server) *grpc.Server {
	return createNewServer(rubyServer, true)
}

// CleanupInternalSocketDir will clean up the directory for internal sockets if it is a generated temp dir
func CleanupInternalSocketDir() {
	if tmpDir := config.GeneratedInternalSocketDir(); tmpDir != "" {
		os.RemoveAll(tmpDir)
	}
}
