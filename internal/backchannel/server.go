package backchannel

import (
	"errors"
	"fmt"
	"net"

	"github.com/hashicorp/yamux"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// ErrNonMultiplexedConnection is returned when attempting to get the peer id of a non-multiplexed
// connection.
var ErrNonMultiplexedConnection = errors.New("non-multiplexed connection")

// ServerHandshaker implements the server side handshake of the multiplexed connection.
type ServerHandshaker struct {
	registry *Registry
	logger   *logrus.Entry
	dialOpts []grpc.DialOption
    setup    func (*ServerHandshaker, net.Conn, credentials.AuthInfo, *yamux.Session) (credentials.AuthInfo, func()(), error)
}

// Magic is used by listenmux to retrieve the magic string for
// backchannel connections.
func (s *ServerHandshaker) Magic() string { return string(magicBytes) }

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

	authInfo, closeFunc, err := s.setup(s, conn, authInfo, muxSession)
	if err != nil {
		logger.Close()
		return nil, nil, err
	}

	// The returned connection must close the underlying network connection, we redirect the close
	// to the muxSession which also closes the underlying connection.
	return connCloser{
			Conn: clientToServerStream,
			close: func() error {
				closeFunc()
				logger.Close()
				return nil
			},
		},
		authInfo,
		nil
}
