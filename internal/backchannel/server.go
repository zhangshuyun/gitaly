package backchannel

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
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

// ServerHandshaker implements the server side handshake of the multiplexed connection.
type ServerHandshaker struct {
	registry *Registry
	logger   *logrus.Entry
	credentials.TransportCredentials
}

// NewServerHandshaker returns a new server side implementation of the backchannel.
func NewServerHandshaker(logger *logrus.Entry, tc credentials.TransportCredentials, reg *Registry) credentials.TransportCredentials {
	return ServerHandshaker{
		TransportCredentials: tc,
		registry:             reg,
		logger:               logger,
	}
}

// restoredConn allows for restoring the connection's stream after peeking it. If the connection
// was not multiplexed, the peeked bytes are restored back into the stream.
type restoredConn struct {
	net.Conn
	reader io.Reader
}

func (rc *restoredConn) Read(b []byte) (int, error) { return rc.reader.Read(b) }

// ServerHandshake peeks the connection to determine whether the client supports establishing a
// backchannel by multiplexing the network connection. If so, it establishes a gRPC ClientConn back
// to the client and stores it's ID in the AuthInfo where it can be later accessed by the RPC handlers.
// gRPC sets an IO timeout on the connection before calling ServerHandshake, so we don't have to handle
// timeouts separately.
func (s ServerHandshaker) ServerHandshake(conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn, authInfo, err := s.TransportCredentials.ServerHandshake(conn)
	if err != nil {
		return nil, nil, fmt.Errorf("wrapped server handshake: %w", err)
	}

	peeked, err := ioutil.ReadAll(io.LimitReader(conn, int64(len(magicBytes))))
	if err != nil {
		return nil, nil, fmt.Errorf("peek network stream: %w", err)
	}

	if !bytes.Equal(peeked, magicBytes) {
		// If the client connection is not multiplexed, restore the peeked bytes back into the stream.
		// We also set a 0 peer ID in the authInfo to indicate that the server handshake was attempted
		// but this was not a multiplexed connection.
		return &restoredConn{
			Conn:   conn,
			reader: io.MultiReader(bytes.NewReader(peeked), conn),
		}, authInfo, nil
	}

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
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return muxSession.Open() }),
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
		authInfoWrapper{
			id:       id,
			AuthInfo: authInfo,
		}, nil
}
