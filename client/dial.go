package client

import (
	"context"
	_ "crypto/x509" // ensure x509 package init happens after this package
	"fmt"
	"net"
	"net/url"
	"os"
	"time"

	gitaly_x509 "gitlab.com/gitlab-org/gitaly/internal/x509"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// DefaultDialOpts hold the default DialOptions for connection to Gitaly over UNIX-socket
var DefaultDialOpts = []grpc.DialOption{}

type connectionType int

const (
	invalidConnection connectionType = iota
	tcpConnection
	tlsConnection
	unixConnection
)

func DialContext(ctx context.Context, rawAddress string, connOpts []grpc.DialOption) (*grpc.ClientConn, error) {
	var canonicalAddress string
	var err error

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

		creds := credentials.NewClientTLSFromCert(certPool, "")
		connOpts = append(connOpts, grpc.WithTransportCredentials(creds))

	case tcpConnection:
		canonicalAddress, err = extractHostFromRemoteURL(rawAddress) // Ensure the form: "host:port" ...
		if err != nil {
			return nil, fmt.Errorf("failed to extract host for 'tcp' connection: %w", err)
		}
		connOpts = append(connOpts, grpc.WithInsecure())

	case unixConnection:
		canonicalAddress = rawAddress // This will be overridden by the custom dialer...
		connOpts = append(
			connOpts,
			grpc.WithInsecure(),
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

	// grpc.KeepaliveParams must be specified at least as large as what is allowed by the
	// server-side grpc.KeepaliveEnforcementPolicy
	connOpts = append(connOpts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                20 * time.Second,
		PermitWithoutStream: true,
	}))

	conn, err := grpc.DialContext(ctx, canonicalAddress, connOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to dial %q connection: %w", canonicalAddress, err)
	}

	return conn, nil
}

func Dial(rawAddress string, connOpts []grpc.DialOption) (*grpc.ClientConn, error) {
	return DialContext(context.Background(), rawAddress, connOpts)
}

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

// respectCommonName ensures that the deprecated common name field is not
// ignored.
// For more info: https://golang.org/doc/go1.15#commonname
// TODO: remove this hack in
func respectCommonName() {
	godebug := "GODEBUG"
	os.Setenv(godebug, os.Getenv(godebug)+",x509ignoreCN=0")
}

func init() {
	respectCommonName()
}
