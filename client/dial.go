package client

import (
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc/credentials"

	"net/url"

	"google.golang.org/grpc"
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

// Dial gitaly
func Dial(rawAddress string, connOpts []grpc.DialOption) (*grpc.ClientConn, error) {
	var canonicalAddress string
	var err error

	switch getConnectionType(rawAddress) {
	case invalidConnection:
		return nil, fmt.Errorf("invalid connection string: %s", rawAddress)

	case tlsConnection:
		canonicalAddress, err = extractHostFromRemoteURL(rawAddress) // Ensure the form: "host:port" ...
		if err != nil {
			return nil, err
		}

		certPool, err := systemCertPool()
		if err != nil {
			return nil, err
		}

		creds := credentials.NewClientTLSFromCert(certPool, "")
		connOpts = append(connOpts, grpc.WithTransportCredentials(creds))

	case tcpConnection:
		canonicalAddress, err = extractHostFromRemoteURL(rawAddress) // Ensure the form: "host:port" ...
		if err != nil {
			return nil, err
		}
		connOpts = append(connOpts, grpc.WithInsecure())

	case unixConnection:
		canonicalAddress = rawAddress // This will be overriden by the custom dialer...
		connOpts = append(
			connOpts,
			grpc.WithInsecure(),
			grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
				path, err := extractPathFromSocketURL(addr)
				if err != nil {
					return nil, err
				}

				return net.DialTimeout("unix", path, timeout)
			}),
		)

	}

	conn, err := grpc.Dial(canonicalAddress, connOpts...)
	if err != nil {
		return nil, err
	}

	return conn, nil
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
