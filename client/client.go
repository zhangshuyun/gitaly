package client

import (
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"google.golang.org/grpc"
)

// Client holds a single gRPC connection
type Client struct {
	conn *grpc.ClientConn
}

// NewClient connects to a gRPC-server and returns the Client
func NewClient(addr string) (*Client, error) {
	conn, err := newConnection(addr)
	if err != nil {
		return nil, err
	}

	return &Client{conn: conn}, nil
}

// Close the gRPC-connection
func (cli *Client) Close() error {
	return cli.conn.Close()
}

// Thankfully borrowed from Workhorse

func newConnection(rawAddress string) (*grpc.ClientConn, error) {
	network, addr, err := parseAddress(rawAddress)
	if err != nil {
		return nil, err
	}

	connOpts := []grpc.DialOption{
		grpc.WithInsecure(), // Since we're connecting to Gitaly over UNIX, we don't need to use TLS credentials.
		grpc.WithDialer(func(a string, _ time.Duration) (net.Conn, error) {
			return net.Dial(network, a)
		}),
	}
	conn, err := grpc.Dial(addr, connOpts...)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func parseAddress(rawAddress string) (network, addr string, err error) {
	// Parsing unix:// URL's with url.Parse does not give the result we want
	// so we do it manually.
	for _, prefix := range []string{"unix://", "unix:"} {
		if strings.HasPrefix(rawAddress, prefix) {
			return "unix", strings.TrimPrefix(rawAddress, prefix), nil
		}
	}

	u, err := url.Parse(rawAddress)
	if err != nil {
		return "", "", err
	}

	if u.Scheme != "tcp" {
		return "", "", fmt.Errorf("unknown scheme: %q", rawAddress)
	}
	if u.Host == "" {
		return "", "", fmt.Errorf("network tcp requires host: %q", rawAddress)
	}
	if u.Path != "" {
		return "", "", fmt.Errorf("network tcp should have no path: %q", rawAddress)
	}
	return "tcp", u.Host, nil
}
