package streamrpcs

import (
	"io"
	"net"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/credentials"
)

// The magic bytes used for classification by listenmux
var magicBytes = []byte("streamrpc00")

// ServerHandshaker implements the server side handshake of the multiplexed connection.
type ServerHandshaker struct {
	logger logrus.FieldLogger
}

// NewServerHandshaker returns an implementation of streamrpc server
// handshaker. The provided TransportCredentials are handshaked prior to
// initializing the multiplexing session. This handshaker Gitaly's unary server
// interceptors into the interceptor chain of input StreamRPC server.
func NewServerHandshaker(logger logrus.FieldLogger) *ServerHandshaker {
	return &ServerHandshaker{
		logger: logger,
	}
}

// Magic is used by listenmux to retrieve the magic string for
// streamrpc connections.
func (s *ServerHandshaker) Magic() string { return string(magicBytes) }

// Handshake "steals" the request from Gitaly's main gRPC server during
// connection handshaking phase. Listenmux depends on the first 11-byte magic
// bytes sent by the client, and invoke StreamRPC handshaker accordingly. The
// request is then handled by stream RPC server, and skipped by Gitaly gRPC
// server.
func (s *ServerHandshaker) Handshake(conn net.Conn, authInfo credentials.AuthInfo) (net.Conn, credentials.AuthInfo, error) {
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return nil, nil, err
	}

	token := make([]byte, TokenSizeBytes)
	_, err := io.ReadFull(conn, token)
	if err != nil {
		return nil, nil, err
	}

	if err = globalRegistry.Push(string(token), conn); err != nil {
		return nil, nil, err
	}

	// At this point, the connection is already closed. If the
	// TransportCredentials continues its code path, gRPC constructs a HTTP2
	// server transport to handle the connection. Eventually, it fails and logs
	// several warnings and errors even though the stream RPC call is
	// successful.
	// Fortunately, gRPC has credentials.ErrConnDispatched, indicating that the
	// connection is already dispatched out of gRPC. gRPC should leave it alone
	// and exit in peace.
	return nil, nil, credentials.ErrConnDispatched
}
