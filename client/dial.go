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
	tcpConnection                    = iota
	tlsConnection                    = iota
	unixConnection                   = iota
)

// Dial gitaly
func Dial(rawAddress string, connOpts []grpc.DialOption) (*grpc.ClientConn, error) {
	canonicalAddress, err := parseAddress(rawAddress)
	if err != nil {
		return nil, err
	}

	switch getConnectionType(rawAddress) {
	case invalidConnection:
		return nil, fmt.Errorf("invalid connection string: %s", rawAddress)

	case tlsConnection:
		certPool, err := systemCertPool()
		if err != nil {
			return nil, err
		}

		creds := credentials.NewClientTLSFromCert(certPool, "")
		connOpts = append(connOpts, grpc.WithTransportCredentials(creds))

	case tcpConnection:
		connOpts = append(connOpts, grpc.WithInsecure())

	case unixConnection:
		connOpts = append(
			connOpts,
			grpc.WithInsecure(),
			grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
				u, err := url.Parse(addr)
				if err != nil {
					return nil, err
				}

				return net.DialTimeout("unix", u.Path, timeout)
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
