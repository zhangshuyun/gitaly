package client

import (
	"context"
	"errors"
	"fmt"
	"sync"

	gitalyauth "gitlab.com/gitlab-org/gitaly/v13/auth"
	"google.golang.org/grpc"
)

// Pool is a pool of GRPC connections. Connections created by it are safe for
// concurrent use.
type Pool struct {
	lock           sync.RWMutex
	connsByAddress map[string]*grpc.ClientConn
	dialOptions    []grpc.DialOption
}

// NewPool creates a new connection pool that's ready for use.
func NewPool(dialOptions ...grpc.DialOption) *Pool {
	return &Pool{
		connsByAddress: make(map[string]*grpc.ClientConn),
		dialOptions:    dialOptions,
	}
}

// Close closes all connections tracked by the connection pool.
func (p *Pool) Close() error {
	p.lock.Lock()
	defer p.lock.Unlock()

	var firstError error
	for addr, conn := range p.connsByAddress {
		if err := conn.Close(); err != nil && firstError == nil {
			firstError = err
		}

		delete(p.connsByAddress, addr)
	}

	return firstError
}

// Dial creates a new client connection in case no connection to the given
// address exists already or returns an already established connection. The
// returned address must not be `Close()`d.
func (p *Pool) Dial(ctx context.Context, address, token string) (*grpc.ClientConn, error) {
	return p.getOrCreateConnection(ctx, address, token)
}

func (p *Pool) getOrCreateConnection(ctx context.Context, address, token string) (*grpc.ClientConn, error) {
	if address == "" {
		return nil, errors.New("address is empty")
	}

	p.lock.RLock()
	cc, ok := p.connsByAddress[address]
	p.lock.RUnlock()

	if ok {
		return cc, nil
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	cc, ok = p.connsByAddress[address]
	if ok {
		return cc, nil
	}

	opts := make([]grpc.DialOption, 0, len(p.dialOptions)+1)
	opts = append(opts, p.dialOptions...)
	if token != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(token)))
	}

	cc, err := DialContext(ctx, address, opts)
	if err != nil {
		return nil, fmt.Errorf("could not dial source: %v", err)
	}

	p.connsByAddress[address] = cc

	return cc, nil
}
