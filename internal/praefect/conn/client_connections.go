package conn

import (
	"errors"
	"sync"

	"gitlab.com/gitlab-org/gitaly/internal/middleware/proxytime"

	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/proxy"
	"google.golang.org/grpc"
)

// ErrConnectionNotFound indicates the connection for a given storage has not yet been registered
var ErrConnectionNotFound = errors.New("client connection not found")

// ErrAlreadyRegistered indicates the client connection for a given storage has already been registered
var ErrAlreadyRegistered = errors.New("client connection already registered")

// ClientConnections contains ready to use grpc client connections
type ClientConnections struct {
	connMutex sync.RWMutex
	nodes     map[string]*grpc.ClientConn
}

// NewClientConnections creates a new ClientConnections struct
func NewClientConnections() *ClientConnections {
	return &ClientConnections{
		nodes: make(map[string]*grpc.ClientConn),
	}
}

// RegisterNode will direct traffic to the supplied downstream connection when the storage location
// is encountered.
func (c *ClientConnections) RegisterNode(storageName, listenAddr, token string) error {
	conn, err := client.Dial(listenAddr,
		[]grpc.DialOption{
			grpc.WithDefaultCallOptions(grpc.CallCustomCodec(proxy.Codec())),
			grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials(token)),
			grpc.WithUnaryInterceptor(proxytime.UnaryProxyTime),
			grpc.WithStreamInterceptor(proxytime.StreamProxyTime),
		},
	)
	if err != nil {
		return err
	}

	return c.setConn(storageName, conn)
}

func (c *ClientConnections) setConn(storageName string, conn *grpc.ClientConn) error {
	c.connMutex.Lock()
	if _, ok := c.nodes[storageName]; ok {
		return ErrAlreadyRegistered
	}
	c.nodes[storageName] = conn
	c.connMutex.Unlock()

	return nil
}

// GetConnection gets the grpc client connection based on an address
func (c *ClientConnections) GetConnection(storageName string) (*grpc.ClientConn, error) {
	c.connMutex.RLock()
	cc, ok := c.nodes[storageName]
	c.connMutex.RUnlock()
	if !ok {
		return nil, ErrConnectionNotFound
	}

	return cc, nil
}
