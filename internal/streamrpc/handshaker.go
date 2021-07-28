package streamrpc

import (
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/bootstrap/starter"
	"google.golang.org/grpc/credentials"
)

// The magic bytes used for classification by listenmux
var magicBytes = []byte("streamrpc00")

// DialNet lets Call initiate unencrypted connections. They tend to be used
// with Gitaly's listenmux multiplexer only. After the connection is
// established, streamrpc's 11-byte magic bytes are written into the wire.
// Listemmux peeks into these magic bytes and redirects the request to
// StreamRPC server.
// Please visit internal/listenmux/mux.go for more information
func DialNet(address string) DialFunc {
	return func(t time.Duration) (net.Conn, error) {
		endpoint, err := starter.ParseEndpoint(address)
		if err != nil {
			return nil, err
		}

		// Dial-only deadline
		deadline := time.Now().Add(t)

		dialer := &net.Dialer{Deadline: deadline}
		conn, err := dialer.Dial(endpoint.Name, endpoint.Addr)
		if err != nil {
			return nil, err
		}

		if err = conn.SetDeadline(deadline); err != nil {
			return nil, err
		}
		// Write the magic bytes on the connection so the server knows we're
		// about to initiate a multiplexing session.
		if _, err := conn.Write(magicBytes); err != nil {
			return nil, fmt.Errorf("streamrpc client: write backchannel magic bytes: %w", err)
		}

		// Reset deadline of tls connection for later stages
		if err = conn.SetDeadline(time.Time{}); err != nil {
			return nil, err
		}

		return conn, nil
	}
}

// DialTLS lets Call initiate TLS connections. Similar to DialNet, the
// connections are used for listenmux multiplexer. There are 3 steps involving:
// - TCP handshake
// - TLS handshake
// - Write streamrpc magic bytes
func DialTLS(address string, cfg *tls.Config) DialFunc {
	return func(t time.Duration) (net.Conn, error) {
		// Dial-only deadline
		deadline := time.Now().Add(t)

		dialer := &net.Dialer{Deadline: deadline}
		tlsConn, err := tls.DialWithDialer(dialer, "tcp", address, cfg)
		if err != nil {
			return nil, err
		}

		err = tlsConn.SetDeadline(deadline)
		if err != nil {
			return nil, err
		}
		// Write the magic bytes on the connection so the server knows we're
		// about to initiate a multiplexing session.
		if _, err := tlsConn.Write(magicBytes); err != nil {
			return nil, fmt.Errorf("streamrpc client: write backchannel magic bytes: %w", err)
		}

		// Reset deadline of tls connection for later stages
		if err = tlsConn.SetDeadline(time.Time{}); err != nil {
			return nil, err
		}

		return tlsConn, nil
	}
}

// ServerHandshaker implements the server side handshake of the multiplexed connection.
type ServerHandshaker struct {
	server *Server
	logger logrus.FieldLogger
}

// NewServerHandshaker returns an implementation of streamrpc server
// handshaker. The provided TransportCredentials are handshaked prior to
// initializing the multiplexing session. This handshaker Gitaly's unary server
// interceptors into the interceptor chain of input StreamRPC server.
func NewServerHandshaker(server *Server, logger logrus.FieldLogger) *ServerHandshaker {
	return &ServerHandshaker{
		server: server,
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

	go func() {
		if err := s.server.Handle(conn); err != nil {
			s.logger.WithError(err).Error("streamrpc: handle call")
		}
	}()
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
