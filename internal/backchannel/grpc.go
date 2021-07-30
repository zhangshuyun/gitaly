package backchannel

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/hashicorp/yamux"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// authInfoWrapper is used to pass the peer id through the context to the RPC handlers.
type authInfoWrapper struct {
	id ID
	credentials.AuthInfo
}

func (w authInfoWrapper) peerID() ID { return w.id }

// GetPeerID gets the ID of the current peer connection.
func GetPeerID(ctx context.Context) (ID, error) {
	peerInfo, ok := peer.FromContext(ctx)
	if !ok {
		return 0, errors.New("no peer info in context")
	}

	wrapper, ok := peerInfo.AuthInfo.(interface{ peerID() ID })
	if !ok {
		return 0, ErrNonMultiplexedConnection
	}

	return wrapper.peerID(), nil
}

// WithID stores the ID in the provided AuthInfo so it can be later accessed by the RPC handler.
// This is exported to facilitate testing.
func WithID(authInfo credentials.AuthInfo, id ID) credentials.AuthInfo {
	return authInfoWrapper{id: id, AuthInfo: authInfo}
}

// NewGRPCHandshaker returns a new server side implementation of the backchannel. The provided TransportCredentials
// are handshaked prior to initializing the multiplexing session. The Registry is used to store the backchannel connections.
// DialOptions can be used to set custom dial options for the backchannel connections. They must not contain a dialer or
// transport credentials as those set by the handshaker.
func NewGRPCHandshaker(logger *logrus.Entry, reg *Registry, dialOpts []grpc.DialOption) *ServerHandshaker {
	return &ServerHandshaker{registry: reg, logger: logger, dialOpts: dialOpts, setup: gRPCSetup}
}

func gRPCSetup(s *ServerHandshaker, conn net.Conn, authInfo credentials.AuthInfo, muxSession *yamux.Session) (credentials.AuthInfo, func()(), error) {
	// The address does not actually matter but we set it so clientConn.Target returns a meaningful value.
	// WithInsecure is used as the multiplexer operates within a TLS session already if one is configured.
	backchannelConn, err := grpc.Dial(
		"multiplexed/"+conn.RemoteAddr().String(),
		append(
			s.dialOpts,
			grpc.WithInsecure(),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return muxSession.Open() }),
		)...,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("dial backchannel: %w", err)
	}

	id := s.registry.RegisterBackchannel(backchannelConn)
	return WithID(authInfo, id), func() {
		s.registry.RemoveBackchannel(id)
		backchannelConn.Close()
		muxSession.Close()
	}, nil
}
