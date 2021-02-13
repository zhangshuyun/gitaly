/*Package praefect is a Gitaly reverse proxy for transparently routing gRPC
calls to a set of Gitaly services.*/
package praefect

import (
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/server/auth"
	"gitlab.com/gitlab-org/gitaly/internal/helper/fieldextractors"
	"gitlab.com/gitlab-org/gitaly/internal/log"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/cancelhandler"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/metadatahandler"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/panichandler"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/sentryhandler"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/proxy"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/middleware"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/service/info"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/service/server"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/service/transaction"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/transactions"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	grpccorrelation "gitlab.com/gitlab-org/labkit/correlation/grpc"
	grpctracing "gitlab.com/gitlab-org/labkit/tracing/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
)

// NewGRPCServer returns gRPC server with registered proxy-handler and actual services praefect serves on its own.
// It includes a set of unary and stream interceptors required to add logging, authentication, etc.
func NewGRPCServer(
	conf config.Config,
	logger *logrus.Entry,
	registry *protoregistry.Registry,
	director proxy.StreamDirector,
	nodeMgr nodes.Manager,
	txMgr *transactions.Manager,
	queue datastore.ReplicationEventQueue,
	rs datastore.RepositoryStore,
	rfs info.ReplicationFactorSetter,
	grpcOpts ...grpc.ServerOption,
) *grpc.Server {
	ctxTagOpts := []grpc_ctxtags.Option{
		grpc_ctxtags.WithFieldExtractorForInitialReq(fieldextractors.FieldExtractor),
	}

	streamInterceptors := []grpc.StreamServerInterceptor{
		grpc_ctxtags.StreamServerInterceptor(ctxTagOpts...),
		grpccorrelation.StreamServerCorrelationInterceptor(), // Must be above the metadata handler
		middleware.MethodTypeStreamInterceptor(registry),
		metadatahandler.StreamInterceptor,
		grpc_prometheus.StreamServerInterceptor,
		grpc_logrus.StreamServerInterceptor(logger,
			grpc_logrus.WithTimestampFormat(log.LogTimestampFormat)),
		sentryhandler.StreamLogHandler,
		cancelhandler.Stream, // Should be below LogHandler
		grpctracing.StreamServerTracingInterceptor(),
		auth.StreamServerInterceptor(conf.Auth),
		// Panic handler should remain last so that application panics will be
		// converted to errors and logged
		panichandler.StreamPanicHandler,
	}

	if conf.Failover.ElectionStrategy == config.ElectionStrategyPerRepository {
		streamInterceptors = append(streamInterceptors, RepositoryExistsStreamInterceptor(rs))
	}

	grpcOpts = append(grpcOpts, proxyRequiredOpts(director)...)
	grpcOpts = append(grpcOpts, []grpc.ServerOption{
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			grpc_ctxtags.UnaryServerInterceptor(ctxTagOpts...),
			grpccorrelation.UnaryServerCorrelationInterceptor(), // Must be above the metadata handler
			middleware.MethodTypeUnaryInterceptor(registry),
			metadatahandler.UnaryInterceptor,
			grpc_prometheus.UnaryServerInterceptor,
			grpc_logrus.UnaryServerInterceptor(logger,
				grpc_logrus.WithTimestampFormat(log.LogTimestampFormat)),
			sentryhandler.UnaryLogHandler,
			cancelhandler.Unary, // Should be below LogHandler
			grpctracing.UnaryServerTracingInterceptor(),
			auth.UnaryServerInterceptor(conf.Auth),
			// Panic handler should remain last so that application panics will be
			// converted to errors and logged
			panichandler.UnaryPanicHandler,
		)),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             20 * time.Second,
			PermitWithoutStream: true,
		}),
	}...)

	warnDupeAddrs(logger, conf)

	srv := grpc.NewServer(grpcOpts...)
	registerServices(srv, nodeMgr, txMgr, conf, queue, rs, rfs)
	return srv
}

func proxyRequiredOpts(director proxy.StreamDirector) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.CustomCodec(proxy.NewCodec()),
		grpc.UnknownServiceHandler(proxy.TransparentHandler(director)),
	}
}

// registerServices registers services praefect needs to handle RPCs on its own.
func registerServices(srv *grpc.Server, nm nodes.Manager, tm *transactions.Manager, conf config.Config, queue datastore.ReplicationEventQueue, rs datastore.RepositoryStore, rfs info.ReplicationFactorSetter) {
	// ServerServiceServer is necessary for the ServerInfo RPC
	gitalypb.RegisterServerServiceServer(srv, server.NewServer(conf, nm))
	gitalypb.RegisterPraefectInfoServiceServer(srv, info.NewServer(nm, conf, queue, rs, rfs))
	gitalypb.RegisterRefTransactionServer(srv, transaction.NewServer(tm))
	healthpb.RegisterHealthServer(srv, health.NewServer())

	grpc_prometheus.Register(srv)
}

func warnDupeAddrs(logger logrus.FieldLogger, conf config.Config) {
	var fishy bool

	for _, virtualStorage := range conf.VirtualStorages {
		addrSet := map[string]struct{}{}
		for _, n := range virtualStorage.Nodes {
			_, ok := addrSet[n.Address]
			if ok {
				logger.Warnf("more than one backend node is hosted at %s", n.Address)
				fishy = true
				continue
			}
			addrSet[n.Address] = struct{}{}
		}
		if fishy {
			logger.Warnf("your Praefect configuration may not offer actual redundancy")
		}
	}
}
