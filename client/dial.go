package client

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/internal/gitaly/client"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// DefaultDialOpts hold the default DialOptions for connection to Gitaly over UNIX-socket
var DefaultDialOpts = []grpc.DialOption{}

// DialContext dials the Gitaly at the given address with the provided options. Valid address formats are
// 'unix:<socket path>' for Unix sockets, 'tcp://<host:port>' for insecure TCP connections and 'tls://<host:port>'
// for TCP+TLS connections.
//
// The returned ClientConns are configured with tracing and correlation id interceptors to ensure they are propagated
// correctly. They're also configured to send Keepalives with settings matching what Gitaly expects.
//
// connOpts should not contain `grpc.WithInsecure` as DialContext determines whether it is needed or not from the
// scheme. `grpc.TransportCredentials` should not be provided either as those are handled internally as well.
func DialContext(ctx context.Context, rawAddress string, connOpts []grpc.DialOption) (*grpc.ClientConn, error) {
	return client.Dial(ctx, rawAddress, connOpts, nil)
}

// Dial calls DialContext with the provided arguments and context.Background. Refer to DialContext's documentation
// for details.
func Dial(rawAddress string, connOpts []grpc.DialOption) (*grpc.ClientConn, error) {
	return DialContext(context.Background(), rawAddress, connOpts)
}

// FailOnNonTempDialError helps to identify if remote listener is ready to accept new connections.
func FailOnNonTempDialError() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithBlock(),
		grpc.FailOnNonTempDialError(true),
	}
}

// HealthCheckDialer uses provided dialer as an actual dialer, but issues a health check request to the remote
// to verify the connection was set properly and could be used with no issues.
func HealthCheckDialer(base Dialer) Dialer {
	return func(ctx context.Context, address string, dialOptions []grpc.DialOption) (*grpc.ClientConn, error) {
		cc, err := base(ctx, address, dialOptions)
		if err != nil {
			return nil, err
		}

		if _, err := healthpb.NewHealthClient(cc).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
			cc.Close()
			return nil, err
		}

		return cc, nil
	}
}
