package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	grpcmw "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcmwlogrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	grpcmwtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	diskcache "gitlab.com/gitlab-org/gitaly/v14/internal/cache"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/server/auth"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/fieldextractors"
	"gitlab.com/gitlab-org/gitaly/v14/internal/listenmux"
	gitalylog "gitlab.com/gitlab-org/gitaly/v14/internal/log"
	"gitlab.com/gitlab-org/gitaly/v14/internal/logsanitizer"
	"gitlab.com/gitlab-org/gitaly/v14/internal/middleware/cache"
	"gitlab.com/gitlab-org/gitaly/v14/internal/middleware/cancelhandler"
	"gitlab.com/gitlab-org/gitaly/v14/internal/middleware/commandstatshandler"
	"gitlab.com/gitlab-org/gitaly/v14/internal/middleware/limithandler"
	"gitlab.com/gitlab-org/gitaly/v14/internal/middleware/metadatahandler"
	"gitlab.com/gitlab-org/gitaly/v14/internal/middleware/panichandler"
	"gitlab.com/gitlab-org/gitaly/v14/internal/middleware/sentryhandler"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/protoregistry"
	grpccorrelation "gitlab.com/gitlab-org/labkit/correlation/grpc"
	grpctracing "gitlab.com/gitlab-org/labkit/tracing/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

func concurrencyKeyFn(ctx context.Context) string {
	tags := grpcmwtags.Extract(ctx)
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

	// grpc-go gets a custom logger; it is too chatty
	grpcmwlogrus.ReplaceGrpcLogger(gitalylog.GrpcGo())
}

// New returns a GRPC server instance with a set of interceptors configured.
// If logrusEntry is nil the default logger will be used.
func New(
	secure bool,
	cfg config.Cfg,
	logrusEntry *log.Entry,
	registry *backchannel.Registry,
	cacheInvalidator diskcache.Invalidator,
) (*grpc.Server, error) {
	ctxTagOpts := []grpcmwtags.Option{
		grpcmwtags.WithFieldExtractorForInitialReq(fieldextractors.FieldExtractor),
	}

	lh := limithandler.New(concurrencyKeyFn)

	transportCredentials := insecure.NewCredentials()
	// If tls config is specified attempt to extract tls options and use it
	// as a grpc.ServerOption
	if secure {
		cert, err := tls.LoadX509KeyPair(cfg.TLS.CertPath, cfg.TLS.KeyPath)
		if err != nil {
			return nil, fmt.Errorf("error reading certificate and key paths: %v", err)
		}

		transportCredentials = credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		})
	}

	lm := listenmux.New(transportCredentials)
	lm.Register(backchannel.NewServerHandshaker(
		logrusEntry,
		registry,
		[]grpc.DialOption{client.UnaryInterceptor()},
	))

	opts := []grpc.ServerOption{
		grpc.Creds(lm),
		grpc.StreamInterceptor(grpcmw.ChainStreamServer(
			grpcmwtags.StreamServerInterceptor(ctxTagOpts...),
			grpccorrelation.StreamServerCorrelationInterceptor(), // Must be above the metadata handler
			metadatahandler.StreamInterceptor,
			grpcprometheus.StreamServerInterceptor,
			commandstatshandler.StreamInterceptor,
			grpcmwlogrus.StreamServerInterceptor(logrusEntry,
				grpcmwlogrus.WithTimestampFormat(gitalylog.LogTimestampFormat),
				grpcmwlogrus.WithMessageProducer(commandstatshandler.CommandStatsMessageProducer)),
			sentryhandler.StreamLogHandler,
			cancelhandler.Stream, // Should be below LogHandler
			auth.StreamServerInterceptor(cfg.Auth),
			lh.StreamInterceptor(), // Should be below auth handler to prevent v2 hmac tokens from timing out while queued
			grpctracing.StreamServerTracingInterceptor(),
			cache.StreamInvalidator(cacheInvalidator, protoregistry.GitalyProtoPreregistered),
			// Panic handler should remain last so that application panics will be
			// converted to errors and logged
			panichandler.StreamPanicHandler,
		)),
		grpc.UnaryInterceptor(grpcmw.ChainUnaryServer(
			grpcmwtags.UnaryServerInterceptor(ctxTagOpts...),
			grpccorrelation.UnaryServerCorrelationInterceptor(), // Must be above the metadata handler
			metadatahandler.UnaryInterceptor,
			grpcprometheus.UnaryServerInterceptor,
			commandstatshandler.UnaryInterceptor,
			grpcmwlogrus.UnaryServerInterceptor(logrusEntry,
				grpcmwlogrus.WithTimestampFormat(gitalylog.LogTimestampFormat),
				grpcmwlogrus.WithMessageProducer(commandstatshandler.CommandStatsMessageProducer)),
			sentryhandler.UnaryLogHandler,
			cancelhandler.Unary, // Should be below LogHandler
			auth.UnaryServerInterceptor(cfg.Auth),
			lh.UnaryInterceptor(), // Should be below auth handler to prevent v2 hmac tokens from timing out while queued
			grpctracing.UnaryServerTracingInterceptor(),
			cache.UnaryInvalidator(cacheInvalidator, protoregistry.GitalyProtoPreregistered),
			// Panic handler should remain last so that application panics will be
			// converted to errors and logged
			panichandler.UnaryPanicHandler,
		)),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             20 * time.Second,
			PermitWithoutStream: true,
		}),
	}

	return grpc.NewServer(opts...), nil
}
