/*Package praefect is a Gitaly reverse proxy for transparently routing gRPC
calls to a set of Gitaly services.*/
package praefect

import (
	"context"
	"net"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/cancelhandler"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/metadatahandler"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/panichandler"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/sentryhandler"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/conn"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/proxy"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/service/server"
	"gitlab.com/gitlab-org/gitaly/internal/server/auth"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	grpccorrelation "gitlab.com/gitlab-org/labkit/correlation/grpc"
	grpctracing "gitlab.com/gitlab-org/labkit/tracing/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Server is a praefect server
type Server struct {
	clientConnections *conn.ClientConnections
	repl              ReplMgr
	s                 *grpc.Server
	conf              config.Config
	l                 *logrus.Entry
}

func (srv *Server) warnDupeAddrs(c config.Config) {
	addrSet := map[string]struct{}{}
	fishy := false
	for _, n := range c.Nodes {
		_, ok := addrSet[n.Address]
		if ok {
			srv.l.Warnf("more than one backend node is hosted at %s", n.Address)
			fishy = true
		}
		addrSet[n.Address] = struct{}{}
	}
	if fishy {
		srv.l.Warnf("your Praefect configuration may not offer actual redundancy")
	}
}

// NewServer returns an initialized praefect gPRC proxy server configured
// with the provided gRPC server options
func NewServer(c *Coordinator, repl ReplMgr, grpcOpts []grpc.ServerOption, l *logrus.Entry, clientConnections *conn.ClientConnections, conf config.Config) *Server {
	grpcOpts = append(grpcOpts, proxyRequiredOpts(c.streamDirector)...)
	grpcOpts = append(grpcOpts, []grpc.ServerOption{
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			grpccorrelation.StreamServerCorrelationInterceptor(), // Must be above the metadata handler
			grpc_prometheus.StreamServerInterceptor,
			grpc_logrus.StreamServerInterceptor(l),
			sentryhandler.StreamLogHandler,
			cancelhandler.Stream, // Should be below LogHandler
			grpctracing.StreamServerTracingInterceptor(),
			auth.StreamServerInterceptor(conf.Auth),
			// Panic handler should remain last so that application panics will be
			// converted to errors and logged
			panichandler.StreamPanicHandler,
		)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			grpccorrelation.UnaryServerCorrelationInterceptor(), // Must be above the metadata handler
			metadatahandler.UnaryInterceptor,
			grpc_prometheus.UnaryServerInterceptor,
			grpc_logrus.UnaryServerInterceptor(l),
			sentryhandler.UnaryLogHandler,
			cancelhandler.Unary, // Should be below LogHandler
			grpctracing.UnaryServerTracingInterceptor(),
			auth.UnaryServerInterceptor(conf.Auth),
			// Panic handler should remain last so that application panics will be
			// converted to errors and logged
			panichandler.UnaryPanicHandler,
		)),
	}...)

	s := &Server{
		s:                 grpc.NewServer(grpcOpts...),
		repl:              repl,
		clientConnections: clientConnections,
		conf:              conf,
		l:                 l,
	}

	s.warnDupeAddrs(conf)

	return s
}

func proxyRequiredOpts(director proxy.StreamDirector) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.CustomCodec(proxy.Codec()),
		grpc.UnknownServiceHandler(proxy.TransparentHandler(director)),
	}
}

// Start will start the praefect gRPC proxy server listening at the provided
// listener. Function will block until the server is stopped or an
// unrecoverable error occurs.
func (srv *Server) Start(lis net.Listener) error {
	srv.registerServices()

	return srv.s.Serve(lis)
}

// registerServices will register any services praefect needs to handle rpcs on its own
func (srv *Server) registerServices() {
	// ServerServiceServer is necessary for the ServerInfo RPC
	gitalypb.RegisterServerServiceServer(srv.s, server.NewServer(srv.conf, srv.clientConnections))

	healthpb.RegisterHealthServer(srv.s, health.NewServer())
}

// Shutdown will attempt a graceful shutdown of the grpc server. If unable
// to gracefully shutdown within the context deadline, it will then
// forcefully shutdown the server and return a context cancellation error.
func (srv *Server) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		srv.s.GracefulStop()
		close(done)
	}()

	select {
	case <-ctx.Done():
		srv.s.Stop()
		return ctx.Err()
	case <-done:
		return nil
	}
}
