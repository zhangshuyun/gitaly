package server

import (
	"context"
	"crypto/tls"
	"os"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	log "github.com/sirupsen/logrus"
	diskcache "gitlab.com/gitlab-org/gitaly/v13/internal/cache"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/server/auth"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v13/internal/helper/fieldextractors"
	gitalylog "gitlab.com/gitlab-org/gitaly/v13/internal/log"
	"gitlab.com/gitlab-org/gitaly/v13/internal/logsanitizer"
	"gitlab.com/gitlab-org/gitaly/v13/internal/middleware/cache"
	"gitlab.com/gitlab-org/gitaly/v13/internal/middleware/cancelhandler"
	"gitlab.com/gitlab-org/gitaly/v13/internal/middleware/commandstatshandler"
	"gitlab.com/gitlab-org/gitaly/v13/internal/middleware/limithandler"
	"gitlab.com/gitlab-org/gitaly/v13/internal/middleware/metadatahandler"
	"gitlab.com/gitlab-org/gitaly/v13/internal/middleware/panichandler"
	"gitlab.com/gitlab-org/gitaly/v13/internal/middleware/sentryhandler"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/protoregistry"
	grpccorrelation "gitlab.com/gitlab-org/labkit/correlation/grpc"
	grpctracing "gitlab.com/gitlab-org/labkit/tracing/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
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
func createNewServer(rubyServer *rubyserver.Server, hookManager hook.Manager, cfg config.Cfg, secure bool) *grpc.Server {
	ctxTagOpts := []grpc_ctxtags.Option{
		grpc_ctxtags.WithFieldExtractorForInitialReq(fieldextractors.FieldExtractor),
	}

	lh := limithandler.New(concurrencyKeyFn)

	storageLocator := config.NewLocator(cfg)

	opts := []grpc.ServerOption{
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			grpc_ctxtags.StreamServerInterceptor(ctxTagOpts...),
			grpccorrelation.StreamServerCorrelationInterceptor(), // Must be above the metadata handler
			metadatahandler.StreamInterceptor,
			grpc_prometheus.StreamServerInterceptor,
			grpc_logrus.StreamServerInterceptor(logrusEntry),
			commandstatshandler.StreamInterceptor,
			sentryhandler.StreamLogHandler,
			cancelhandler.Stream, // Should be below LogHandler
			auth.StreamServerInterceptor(cfg.Auth),
			lh.StreamInterceptor(), // Should be below auth handler to prevent v2 hmac tokens from timing out while queued
			grpctracing.StreamServerTracingInterceptor(),
			cache.StreamInvalidator(diskcache.LeaseKeyer{}, protoregistry.GitalyProtoPreregistered),
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
			commandstatshandler.UnaryInterceptor,
			sentryhandler.UnaryLogHandler,
			cancelhandler.Unary, // Should be below LogHandler
			auth.UnaryServerInterceptor(cfg.Auth),
			lh.UnaryInterceptor(), // Should be below auth handler to prevent v2 hmac tokens from timing out while queued
			grpctracing.UnaryServerTracingInterceptor(),
			cache.UnaryInvalidator(diskcache.LeaseKeyer{}, protoregistry.GitalyProtoPreregistered),
			// Panic handler should remain last so that application panics will be
			// converted to errors and logged
			panichandler.UnaryPanicHandler,
		)),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             20 * time.Second,
			PermitWithoutStream: true,
		}),
	}

	// If tls config is specified attempt to extract tls options and use it
	// as a grpc.ServerOption
	if secure {
		cert, err := tls.LoadX509KeyPair(cfg.TLS.CertPath, cfg.TLS.KeyPath)
		if err != nil {
			log.Fatalf("error reading certificate and key paths: %v", err)
		}
		opts = append(opts, grpc.Creds(credentials.NewServerTLSFromCert(&cert)))
	}

	server := grpc.NewServer(opts...)

	service.RegisterAll(server, cfg, rubyServer, hookManager, storageLocator)
	reflection.Register(server)

	grpc_prometheus.Register(server)

	return server
}

// NewInsecure returns a GRPC server with all Gitaly services and interceptors set up.
func NewInsecure(rubyServer *rubyserver.Server, hookManager hook.Manager, cfg config.Cfg) *grpc.Server {
	return createNewServer(rubyServer, hookManager, cfg, false)
}

// NewSecure returns a GRPC server enabling TLS credentials
func NewSecure(rubyServer *rubyserver.Server, hookManager hook.Manager, cfg config.Cfg) *grpc.Server {
	return createNewServer(rubyServer, hookManager, cfg, true)
}

// CleanupInternalSocketDir will clean up the directory for internal sockets if it is a generated temp dir
func CleanupInternalSocketDir() {
	if tmpDir := config.GeneratedInternalSocketDir(); tmpDir != "" {
		os.RemoveAll(tmpDir)
	}
}
