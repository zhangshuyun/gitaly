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

// ErrNonMultiplexedConnection is returned when attempting to get the peer id of a non-multiplexed
// connection.
var ErrNonMultiplexedConnection = errors.New("non-multiplexed connection")

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

// ServerHandshaker implements the server side handshake of the multiplexed connection.
type ServerHandshaker struct {
	registry *Registry
	logger   *logrus.Entry
	dialOpts []grpc.DialOption
}

// Magic is used by listenmux to retrieve the magic string for
// backchannel connections.
func (s *ServerHandshaker) Magic() string { return string(magicBytes) }

// NewServerHandshaker returns a new server side implementation of the backchannel. The provided TransportCredentials
// are handshaked prior to initializing the multiplexing session. The Registry is used to store the backchannel connections.
// DialOptions can be used to set custom dial options for the backchannel connections. They must not contain a dialer or
// transport credentials as those set by the handshaker.
func NewServerHandshaker(logger *logrus.Entry, reg *Registry, dialOpts []grpc.DialOption) *ServerHandshaker {
	return &ServerHandshaker{registry: reg, logger: logger, dialOpts: dialOpts}
}

// Handshake establishes a gRPC ClientConn back to the backchannel client
// on the other side and stores its ID in the AuthInfo where it can be
// later accessed by the RPC handlers. gRPC sets an IO timeout on the
// connection before calling ServerHandshake, so we don't have to handle
// timeouts separately.
func (s *ServerHandshaker) Handshake(conn net.Conn, authInfo credentials.AuthInfo) (net.Conn, credentials.AuthInfo, error) {
	// It is not necessary to clean up any of the multiplexing-related sessions on errors as the
	// gRPC server closes the conn if there is an error, which closes the multiplexing
	// session as well.

	logger := s.logger.WriterLevel(logrus.ErrorLevel)

	// Open the server side of the multiplexing session.
	muxSession, err := yamux.Server(conn, muxConfig(logger))
	if err != nil {
		logger.Close()
		return nil, nil, fmt.Errorf("create multiplexing session: %w", err)
	}

	// Accept the client's stream. This is the client's gRPC session to the server.
	clientToServerStream, err := muxSession.Accept()
	if err != nil {
		logger.Close()
		return nil, nil, fmt.Errorf("accept client's stream: %w", err)
	}

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
		logger.Close()
		return nil, nil, fmt.Errorf("dial backchannel: %w", err)
	}

	id := s.registry.RegisterBackchannel(backchannelConn)
	// The returned connection must close the underlying network connection, we redirect the close
	// to the muxSession which also closes the underlying connection.
	return connCloser{
			Conn: clientToServerStream,
			close: func() error {
				s.registry.RemoveBackchannel(id)
				backchannelConn.Close()
				muxSession.Close()
				logger.Close()
				return nil
			},
		},
		WithID(authInfo, id),
		nil
}
