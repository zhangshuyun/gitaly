package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/client"
	diskcache "gitlab.com/gitlab-org/gitaly/internal/cache"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/server/auth"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/internal/helper/fieldextractors"
	gitalylog "gitlab.com/gitlab-org/gitaly/internal/log"
	"gitlab.com/gitlab-org/gitaly/internal/logsanitizer"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/cache"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/cancelhandler"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/commandstatshandler"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/limithandler"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/metadatahandler"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/panichandler"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/sentryhandler"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
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

// registerServices registers all services on the provided gRPC server instance.
func registerServices(
	server *grpc.Server,
	rubyServer *rubyserver.Server,
	hookManager hook.Manager,
	txManager transaction.Manager,
	cfg config.Cfg,
	conns *client.Pool,
	locator storage.Locator,
	gitCmdFactory git.CommandFactory,
) {
	service.RegisterAll(server, cfg, rubyServer, hookManager, txManager, locator, conns, gitCmdFactory)
	reflection.Register(server)
	grpc_prometheus.Register(server)
}

// createNewServer creates a new gRPC server with all required middleware configured.
func createNewServer(cfg config.Cfg, secure bool) (*grpc.Server, error) {
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
			commandstatshandler.StreamInterceptor,
			grpc_logrus.StreamServerInterceptor(logrusEntry,
				grpc_logrus.WithTimestampFormat(gitalylog.LogTimestampFormat),
				grpc_logrus.WithMessageProducer(commandstatshandler.CommandStatsMessageProducer)),
			sentryhandler.StreamLogHandler,
			cancelhandler.Stream, // Should be below LogHandler
			auth.StreamServerInterceptor(cfg.Auth),
			lh.StreamInterceptor(), // Should be below auth handler to prevent v2 hmac tokens from timing out while queued
			grpctracing.StreamServerTracingInterceptor(),
			cache.StreamInvalidator(diskcache.NewLeaseKeyer(storageLocator), protoregistry.GitalyProtoPreregistered),
			// Panic handler should remain last so that application panics will be
			// converted to errors and logged
			panichandler.StreamPanicHandler,
		)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			grpc_ctxtags.UnaryServerInterceptor(ctxTagOpts...),
			grpccorrelation.UnaryServerCorrelationInterceptor(), // Must be above the metadata handler
			metadatahandler.UnaryInterceptor,
			grpc_prometheus.UnaryServerInterceptor,
			commandstatshandler.UnaryInterceptor,
			grpc_logrus.UnaryServerInterceptor(logrusEntry,
				grpc_logrus.WithTimestampFormat(gitalylog.LogTimestampFormat),
				grpc_logrus.WithMessageProducer(commandstatshandler.CommandStatsMessageProducer)),
			sentryhandler.UnaryLogHandler,
			cancelhandler.Unary, // Should be below LogHandler
			auth.UnaryServerInterceptor(cfg.Auth),
			lh.UnaryInterceptor(), // Should be below auth handler to prevent v2 hmac tokens from timing out while queued
			grpctracing.UnaryServerTracingInterceptor(),
			cache.UnaryInvalidator(diskcache.NewLeaseKeyer(storageLocator), protoregistry.GitalyProtoPreregistered),
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
			return nil, fmt.Errorf("error reading certificate and key paths: %v", err)
		}
		opts = append(opts, grpc.Creds(credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		})))
	}

	return grpc.NewServer(opts...), nil
}

// New returns a GRPC server with all Gitaly services and interceptors set.
func New(secure bool, rubyServer *rubyserver.Server, hookManager hook.Manager, txManager transaction.Manager, cfg config.Cfg, conns *client.Pool, locator storage.Locator, gitCmdFactory git.CommandFactory) (*grpc.Server, error) {
	server, err := createNewServer(cfg, secure)
	if err != nil {
		return nil, err
	}
	registerServices(server, rubyServer, hookManager, txManager, cfg, conns, locator, gitCmdFactory)
	return server, nil
}
