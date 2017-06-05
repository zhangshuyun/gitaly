package client

import (
	"net"
	"time"

	"google.golang.org/grpc"
)

// Client holds a single gRPC connection
type Client struct {
	conn *grpc.ClientConn
}

// NewClient connects to a gRPC-server and returns the Client
func NewClient(addr string) (*Client, error) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithDialer(func(addr string, _ time.Duration) (net.Conn, error) {
			return net.Dial("unix", addr)
		}),
	}
	conn, err := grpc.Dial(addr, connOpts...)
	if err != nil {
		return nil, err
	}

	return &Client{conn: conn}, nil
}

// Close the gRPC-connection
func (cli *Client) Close() error {
	return cli.conn.Close()
}
