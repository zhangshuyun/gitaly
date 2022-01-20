package client

import (
	"context"
	"io"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/listenmux"
	"gitlab.com/gitlab-org/gitaly/v14/internal/sidechannel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// SidechannelRegistry associates sidechannel callbacks with outbound
// gRPC calls.
type SidechannelRegistry struct {
	registry *sidechannel.Registry
	logger   *logrus.Entry
}

// NewSidechannelRegistry returns a new registry.
func NewSidechannelRegistry(logger *logrus.Entry) *SidechannelRegistry {
	return &SidechannelRegistry{
		registry: sidechannel.NewRegistry(),
		logger:   logger,
	}
}

// Register registers a callback. It adds metadata to ctx and returns the
// new context. The caller must use the new context for the gRPC call.
// Caller must Close() the returned SidechannelWaiter to prevent resource
// leaks.
func (sr *SidechannelRegistry) Register(
	ctx context.Context,
	callback func(SidechannelConn) error,
) (context.Context, *SidechannelWaiter) {
	ctx, waiter := sidechannel.RegisterSidechannel(
		ctx,
		sr.registry,
		func(cc *sidechannel.ClientConn) error { return callback(cc) },
	)
	return ctx, &SidechannelWaiter{waiter: waiter}
}

// SidechannelWaiter represents a pending sidechannel and its callback.
type SidechannelWaiter struct{ waiter *sidechannel.Waiter }

// Close de-registers the sidechannel callback. If the callback is still
// running, Close blocks until it is done and returns the error return
// value of the callback. If the callback has not been called yet, Close
// returns an error immediately.
func (w *SidechannelWaiter) Close() error { return w.waiter.Close() }

// SidechannelConn allows a client to read and write bytes with less
// overhead than doing so via gRPC messages.
type SidechannelConn interface {
	io.ReadWriter

	// CloseWrite tells the server we won't write any more data. We can still
	// read data from the server after CloseWrite(). A typical use case is in
	// an RPC where the byte stream has a request/response pattern: the
	// client then uses CloseWrite() to signal the end of the request body.
	// When the client calls CloseWrite(), the server receives EOF.
	CloseWrite() error
}

// TestSidechannelServer allows downstream consumers of this package to
// create mock sidechannel gRPC servers.
func TestSidechannelServer(
	logger *logrus.Entry,
	creds credentials.TransportCredentials,
	handler func(interface{}, grpc.ServerStream, io.ReadWriteCloser) error,
) []grpc.ServerOption {
	return []grpc.ServerOption{
		SidechannelServer(logger, creds),
		grpc.UnknownServiceHandler(func(srv interface{}, stream grpc.ServerStream) error {
			conn, err := OpenServerSidechannel(stream.Context())
			if err != nil {
				return err
			}
			defer conn.Close()

			return handler(srv, stream, conn)
		}),
	}
}

// SidechannelServer adds sidechannel support to a gRPC server
func SidechannelServer(logger *logrus.Entry, creds credentials.TransportCredentials) grpc.ServerOption {
	lm := listenmux.New(creds)
	lm.Register(backchannel.NewServerHandshaker(logger, backchannel.NewRegistry(), nil))
	return grpc.Creds(lm)
}

// OpenServerSidechannel opens a sidechannel on the server side. This
// only works if the server was created using SidechannelServer().
func OpenServerSidechannel(ctx context.Context) (io.ReadWriteCloser, error) {
	return sidechannel.OpenSidechannel(ctx)
}
