/*Package praefect is a Gitaly reverse proxy for transparently routing gRPC
calls to a set of Gitaly services.*/
package praefect

import (
	"context"
	"fmt"
	"net"
	"sync"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/mwitkow/grpc-proxy/proxy"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/cancelhandler"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/metadatahandler"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/panichandler"
	grpccorrelation "gitlab.com/gitlab-org/labkit/correlation/grpc"
	grpctracing "gitlab.com/gitlab-org/labkit/tracing/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Logger is a simple interface that allows loggers to be dependency injected
// into the praefect server
type Logger interface {
	Debugf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// Coordinator takes care of directing client requests to the appropriate
// downstream server. The coordinator is thread safe; concurrent calls to
// register nodes are safe.
type Coordinator struct {
	log  Logger
	lock sync.RWMutex

	// Nodes will in the first interations have only one key, which limits
	// the praefect to serve only 1 distinct set of Gitaly nodes.
	// One limitation is that each server needs the same amount of disk
	// space in case of full replication.
	nodes map[string]*grpc.ClientConn
}

// newCoordinator returns a new Coordinator that utilizes the provided logger
func newCoordinator(l Logger) *Coordinator {
	return &Coordinator{
		log:   l,
		nodes: make(map[string]*grpc.ClientConn),
	}
}

// streamDirector determines which downstream servers receive requests
func (c *Coordinator) streamDirector(ctx context.Context, fullMethodName string) (context.Context, *grpc.ClientConn, error) {
	// For phase 1, we need to route messages based on the storage location
	// to the appropriate Gitaly node.
	c.log.Debugf("Stream director received method %s", fullMethodName)

	c.lock.RLock()

	var cc *grpc.ClientConn
	storageLoc := ""
	// We only need the first node, as there's only one storage location per
	// praefect at this time
	for k, v := range c.nodes {
		storageLoc = k
		cc = v
		break
	}
	c.lock.RUnlock()

	if storageLoc == "" {
		err := status.Error(
			codes.FailedPrecondition,
			"no downstream node registered",
		)
		return nil, nil, err
	}

	return ctx, cc, nil
}

// Server is a praefect server
type Server struct {
	*Coordinator
	s *grpc.Server
}

// NewServer returns an initialized praefect gPRC proxy server configured
// with the provided gRPC server options
func NewServer(grpcOpts []grpc.ServerOption, l Logger) *Server {
	c := newCoordinator(l)

	grpcOpts = append(grpcOpts, proxyRequiredOpts(c.streamDirector)...)
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
		Coordinator: c,
	}
}

// RegisterNode will direct traffic to the supplied downstream connection when the storage location
// is encountered.
func (c *Coordinator) RegisterNode(storageLoc, listenAddr string) error {
	conn, err := client.Dial(listenAddr,
		[]grpc.DialOption{grpc.WithDefaultCallOptions(grpc.CallCustomCodec(proxy.Codec()))},
	)
	if err != nil {
		return err
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	if _, ok := c.nodes[storageLoc]; !ok && len(c.nodes) > 0 {
		conn.Close()
		return fmt.Errorf("error: registering %s failed, only one storage location per server is supported", storageLoc)
	}

	c.nodes[storageLoc] = conn

	return nil
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
