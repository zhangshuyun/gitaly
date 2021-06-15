package client

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"time"

	gitaly_x509 "gitlab.com/gitlab-org/gitaly/v14/internal/x509"
	grpccorrelation "gitlab.com/gitlab-org/labkit/correlation/grpc"
	grpctracing "gitlab.com/gitlab-org/labkit/tracing/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

type connectionType int

const (
	invalidConnection connectionType = iota
	tcpConnection
	tlsConnection
	unixConnection
)

func getConnectionType(rawAddress string) connectionType {
	u, err := url.Parse(rawAddress)
	if err != nil {
		return invalidConnection
	}

	switch u.Scheme {
	case "tls":
		return tlsConnection
	case "unix":
		return unixConnection
	case "tcp":
		return tcpConnection
	default:
		return invalidConnection
	}
}

// Handshaker is an interface that allows for wrapping the transport credentials
// with a custom handshake.
type Handshaker interface {
	// ClientHandshake wraps the provided credentials and returns new credentials.
	ClientHandshake(credentials.TransportCredentials) credentials.TransportCredentials
}

// Dial dials a Gitaly node serving at the given address. Dial is used by the public 'client' package
// and the expected behavior is mostly documented there.
//
// If handshaker is provided, it's passed the transport credentials which would be otherwise set. The transport credentials
// returned by handshaker are then set instead.
func Dial(ctx context.Context, rawAddress string, connOpts []grpc.DialOption, handshaker Handshaker) (*grpc.ClientConn, error) {
	var canonicalAddress string
	var err error
	var transportCredentials credentials.TransportCredentials

	switch getConnectionType(rawAddress) {
	case invalidConnection:
		return nil, fmt.Errorf("invalid connection string: %q", rawAddress)

	case tlsConnection:
		canonicalAddress, err = extractHostFromRemoteURL(rawAddress) // Ensure the form: "host:port" ...
		if err != nil {
			return nil, fmt.Errorf("failed to extract host for 'tls' connection: %w", err)
		}

		certPool, err := gitaly_x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("failed to get system certificat pool for 'tls' connection: %w", err)
		}

		transportCredentials = credentials.NewTLS(&tls.Config{
			RootCAs:    certPool,
			MinVersion: tls.VersionTLS12,
		})

	case tcpConnection:
		canonicalAddress, err = extractHostFromRemoteURL(rawAddress) // Ensure the form: "host:port" ...
		if err != nil {
			return nil, fmt.Errorf("failed to extract host for 'tcp' connection: %w", err)
		}

	case unixConnection:
		canonicalAddress = rawAddress // This will be overridden by the custom dialer...
		connOpts = append(
			connOpts,
			// Use a custom dialer to ensure that we don't experience
			// issues in environments that have proxy configurations
			// https://gitlab.com/gitlab-org/gitaly/merge_requests/1072#note_140408512
			grpc.WithContextDialer(func(ctx context.Context, addr string) (conn net.Conn, err error) {
				path, err := extractPathFromSocketURL(addr)
				if err != nil {
					return nil, fmt.Errorf("failed to extract host for 'unix' connection: %w", err)
				}

				d := net.Dialer{}
				return d.DialContext(ctx, "unix", path)
			}),
		)
	}

	if handshaker != nil {
		if transportCredentials == nil {
			transportCredentials = insecure.NewCredentials()
		}

		transportCredentials = handshaker.ClientHandshake(transportCredentials)
	}

	if transportCredentials == nil {
		connOpts = append(connOpts, grpc.WithInsecure())
	} else {
		connOpts = append(connOpts, grpc.WithTransportCredentials(transportCredentials))
	}

	connOpts = append(connOpts,
		// grpc.KeepaliveParams must be specified at least as large as what is allowed by the
		// server-side grpc.KeepaliveEnforcementPolicy
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                20 * time.Second,
			PermitWithoutStream: true,
		}),
		UnaryInterceptor(),
		grpc.WithChainStreamInterceptor(
			grpctracing.StreamClientTracingInterceptor(),
			grpccorrelation.StreamClientCorrelationInterceptor(),
		),
	)

	conn, err := grpc.DialContext(ctx, canonicalAddress, connOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to dial %q connection: %w", canonicalAddress, err)
	}

	return conn, nil
}

// UnaryInterceptor returns the unary interceptors that should be configured for a client.
func UnaryInterceptor() grpc.DialOption {
	return grpc.WithChainUnaryInterceptor(
		grpctracing.UnaryClientTracingInterceptor(),
		grpccorrelation.UnaryClientCorrelationInterceptor(),
	)
}
