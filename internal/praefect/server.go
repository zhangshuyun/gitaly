/*Package praefect is a Gitaly reverse proxy for transparently routing gRPC
calls to a set of Gitaly services.*/
package praefect

import (
	"time"

	grpcmw "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcmwlogrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	grpcmwtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/server/auth"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/fieldextractors"
	"gitlab.com/gitlab-org/gitaly/v14/internal/listenmux"
	"gitlab.com/gitlab-org/gitaly/v14/internal/log"
	"gitlab.com/gitlab-org/gitaly/v14/internal/middleware/cancelhandler"
	"gitlab.com/gitlab-org/gitaly/v14/internal/middleware/metadatahandler"
	"gitlab.com/gitlab-org/gitaly/v14/internal/middleware/panichandler"
	"gitlab.com/gitlab-org/gitaly/v14/internal/middleware/sentryhandler"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/grpc-proxy/proxy"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/middleware"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/service"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/service/info"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/service/server"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/service/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/transactions"
	"gitlab.com/gitlab-org/gitaly/v14/internal/sidechannel"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	grpccorrelation "gitlab.com/gitlab-org/labkit/correlation/grpc"
	grpctracing "gitlab.com/gitlab-org/labkit/tracing/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
)

// NewBackchannelServerFactory returns a ServerFactory that serves the RefTransactionServer on the backchannel
// connection.
func NewBackchannelServerFactory(logger *logrus.Entry, refSvc gitalypb.RefTransactionServer, registry *sidechannel.Registry) backchannel.ServerFactory {
	return func() backchannel.Server {
		lm := listenmux.New(insecure.NewCredentials())
		lm.Register(sidechannel.NewServerHandshaker(registry))
		srv := grpc.NewServer(
			grpc.UnaryInterceptor(grpcmw.ChainUnaryServer(
				commonUnaryServerInterceptors(logger)...,
			)),
			grpc.Creds(lm),
		)
		gitalypb.RegisterRefTransactionServer(srv, refSvc)
		grpcprometheus.Register(srv)
		return srv
	}
}

func commonUnaryServerInterceptors(logger *logrus.Entry) []grpc.UnaryServerInterceptor {
	return []grpc.UnaryServerInterceptor{
		grpcmwtags.UnaryServerInterceptor(ctxtagsInterceptorOption()),
		grpccorrelation.UnaryServerCorrelationInterceptor(), // Must be above the metadata handler
		metadatahandler.UnaryInterceptor,
		grpcprometheus.UnaryServerInterceptor,
		grpcmwlogrus.UnaryServerInterceptor(logger, grpcmwlogrus.WithTimestampFormat(log.LogTimestampFormat)),
		sentryhandler.UnaryLogHandler,
		cancelhandler.Unary, // Should be below LogHandler
		grpctracing.UnaryServerTracingInterceptor(),
		// Panic handler should remain last so that application panics will be
		// converted to errors and logged
		panichandler.UnaryPanicHandler,
	}
}

func ctxtagsInterceptorOption() grpcmwtags.Option {
	return grpcmwtags.WithFieldExtractorForInitialReq(fieldextractors.FieldExtractor)
}

// NewGRPCServer returns gRPC server with registered proxy-handler and actual services praefect serves on its own.
// It includes a set of unary and stream interceptors required to add logging, authentication, etc.
func NewGRPCServer(
	conf config.Config,
	logger *logrus.Entry,
	registry *protoregistry.Registry,
	director proxy.StreamDirector,
	txMgr *transactions.Manager,
	rs datastore.RepositoryStore,
	assignmentStore AssignmentStore,
	conns Connections,
	primaryGetter PrimaryGetter,
	creds credentials.TransportCredentials,
	grpcOpts ...grpc.ServerOption,
) *grpc.Server {
	streamInterceptors := []grpc.StreamServerInterceptor{
		grpcmwtags.StreamServerInterceptor(ctxtagsInterceptorOption()),
		grpccorrelation.StreamServerCorrelationInterceptor(), // Must be above the metadata handler
		middleware.MethodTypeStreamInterceptor(registry),
		metadatahandler.StreamInterceptor,
		grpcprometheus.StreamServerInterceptor,
		grpcmwlogrus.StreamServerInterceptor(logger,
			grpcmwlogrus.WithTimestampFormat(log.LogTimestampFormat)),
		sentryhandler.StreamLogHandler,
		cancelhandler.Stream, // Should be below LogHandler
		grpctracing.StreamServerTracingInterceptor(),
		auth.StreamServerInterceptor(conf.Auth),
		// Panic handler should remain last so that application panics will be
		// converted to errors and logged
		panichandler.StreamPanicHandler,
	}

	grpcOpts = append(grpcOpts, proxyRequiredOpts(director)...)
	grpcOpts = append(grpcOpts, []grpc.ServerOption{
		grpc.StreamInterceptor(grpcmw.ChainStreamServer(streamInterceptors...)),
		grpc.UnaryInterceptor(grpcmw.ChainUnaryServer(
			append(
				commonUnaryServerInterceptors(logger),
				middleware.MethodTypeUnaryInterceptor(registry),
				auth.UnaryServerInterceptor(conf.Auth),
			)...,
		)),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             20 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time: 5 * time.Minute,
		}),
	}...)

	// Accept backchannel connections so that we can proxy sidechannels
	// from clients (e.g. Workhorse) to a backend Gitaly server.
	if creds == nil {
		creds = insecure.NewCredentials()
	}
	lm := listenmux.New(creds)
	lm.Register(backchannel.NewServerHandshaker(logger, backchannel.NewRegistry(), nil))
	grpcOpts = append(grpcOpts, grpc.Creds(lm))

	warnDupeAddrs(logger, conf)

	srv := grpc.NewServer(grpcOpts...)
	registerServices(srv, txMgr, conf, rs, assignmentStore, service.Connections(conns), primaryGetter)

	if conf.Failover.ElectionStrategy == config.ElectionStrategyPerRepository {
		proxy.RegisterStreamHandlers(srv, "gitaly.RepositoryService", map[string]grpc.StreamHandler{
			"RepositoryExists": RepositoryExistsHandler(rs),
			"RemoveRepository": RemoveRepositoryHandler(rs, conns),
		})
		proxy.RegisterStreamHandlers(srv, "gitaly.ObjectPoolService", map[string]grpc.StreamHandler{
			"DeleteObjectPool": DeleteObjectPoolHandler(rs, conns),
		})
	}

	return srv
}

func proxyRequiredOpts(director proxy.StreamDirector) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ForceServerCodec(proxy.NewCodec()),
		grpc.UnknownServiceHandler(proxy.TransparentHandler(director)),
	}
}

// registerServices registers services praefect needs to handle RPCs on its own.
func registerServices(
	srv *grpc.Server,
	tm *transactions.Manager,
	conf config.Config,
	rs datastore.RepositoryStore,
	assignmentStore AssignmentStore,
	conns service.Connections,
	primaryGetter info.PrimaryGetter,
) {
	// ServerServiceServer is necessary for the ServerInfo RPC
	gitalypb.RegisterServerServiceServer(srv, server.NewServer(conf, conns))
	gitalypb.RegisterPraefectInfoServiceServer(srv, info.NewServer(conf, rs, assignmentStore, conns, primaryGetter))
	gitalypb.RegisterRefTransactionServer(srv, transaction.NewServer(tm))
	healthpb.RegisterHealthServer(srv, health.NewServer())

	grpcprometheus.Register(srv)
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
