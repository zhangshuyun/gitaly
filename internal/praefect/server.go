/*Package praefect is a Gitaly reverse proxy for transparently routing gRPC
calls to a set of Gitaly services.*/
package praefect

import (
	"context"
	"net"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/cancelhandler"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/metadatahandler"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/panichandler"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/proxy"
	grpccorrelation "gitlab.com/gitlab-org/labkit/correlation/grpc"
	grpctracing "gitlab.com/gitlab-org/labkit/tracing/grpc"
	"google.golang.org/grpc"
)

// Server is a praefect server
type Server struct {
	coordinator *Coordinator
	repl        ReplMgr
	s           *grpc.Server
}

// NewServer returns an initialized praefect gPRC proxy server configured
// with the provided gRPC server options
func NewServer(c *Coordinator, repl ReplMgr, grpcOpts []grpc.ServerOption, l *logrus.Entry) *Server {
	grpcOpts = append(grpcOpts, proxyRequiredOpts(c.streamDirector, c.connDownHandler)...)
	grpcOpts = append(grpcOpts, []grpc.ServerOption{
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			grpccorrelation.StreamServerCorrelationInterceptor(), // Must be above the metadata handler
			grpc_prometheus.StreamServerInterceptor,
			cancelhandler.Stream, // Should be below LogHandler
			grpctracing.StreamServerTracingInterceptor(),
			// Panic handler should remain last so that application panics will be
			// converted to errors and logged
			panichandler.StreamPanicHandler,
		)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			grpccorrelation.UnaryServerCorrelationInterceptor(), // Must be above the metadata handler
			metadatahandler.UnaryInterceptor,
			grpc_prometheus.UnaryServerInterceptor,
			cancelhandler.Unary, // Should be below LogHandler
			grpctracing.UnaryServerTracingInterceptor(),
			// Panic handler should remain last so that application panics will be
			// converted to errors and logged
			panichandler.UnaryPanicHandler,
		)),
	}...)

	return &Server{
		s:           grpc.NewServer(grpcOpts...),
		coordinator: c,
		repl:        repl,
	}
}

func proxyRequiredOpts(director proxy.StreamDirector, connDownHandler proxy.ConnectionDownNotifier) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.CustomCodec(proxy.Codec()),
		grpc.UnknownServiceHandler(proxy.TransparentHandler(director, connDownHandler)),
	}
}

// Start will start the praefect gRPC proxy server listening at the provided
// listener. Function will block until the server is stopped or an
// unrecoverable error occurs.
func (srv *Server) Start(lis net.Listener) error {
	return srv.s.Serve(lis)
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
