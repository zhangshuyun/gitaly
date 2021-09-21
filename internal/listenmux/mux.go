package listenmux

import (
	"bytes"
	"fmt"
	"io"
	"net"

	"google.golang.org/grpc/credentials"
)

const magicLen = 11

// Handshaker represents a multiplexed connection type.
type Handshaker interface {
	// Handshake is called with a valid Conn and AuthInfo when grpc-go
	// accepts a connection that presents the Magic() magic bytes. From the
	// point of view of grpc-go, it is part of the
	// credentials.TransportCredentials.ServerHandshake callback. The return
	// values of Handshake become the return values of
	// credentials.TransportCredentials.ServerHandshake.
	Handshake(net.Conn, credentials.AuthInfo) (net.Conn, credentials.AuthInfo, error)

	// Magic returns the magic bytes that clients must send on the wire to
	// reach this handshaker. The string must be no more than 11 bytes long.
	// If it is longer, this handshaker will never be called.
	Magic() string
}

var _ credentials.TransportCredentials = &Mux{}

// Mux is a listener multiplexer that plugs into grpc-go as a "TransportCredentials" callback.
type Mux struct {
	credentials.TransportCredentials
	handshakers map[string]Handshaker
}

// Register registers a handshaker. It is not thread-safe.
func (m *Mux) Register(h Handshaker) {
	if len(h.Magic()) != magicLen {
		panic("wrong magic bytes length")
	}

	m.handshakers[h.Magic()] = h
}

// New returns a *Mux that wraps existing transport credentials. This
// does nothing interesting unless you also call Register to add
// handshakers to the Mux.
func New(tc credentials.TransportCredentials) *Mux {
	return &Mux{
		TransportCredentials: tc,
		handshakers:          make(map[string]Handshaker),
	}
}

// restoredConn allows for restoring the connection's stream after peeking it. If the connection
// was not multiplexed, the peeked bytes are restored back into the stream.
type restoredConn struct {
	net.Conn
	reader io.Reader
}

func (rc *restoredConn) Read(b []byte) (int, error) { return rc.reader.Read(b) }

// ServerHandshake peeks the connection to determine whether the client
// wants to make a multiplexed connection. It is part of the
// TransportCredentials interface.
func (m *Mux) ServerHandshake(conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn, authInfo, err := m.TransportCredentials.ServerHandshake(conn)
	if err != nil {
		return nil, nil, fmt.Errorf("wrapped server handshake: %w", err)
	}

	peeked, err := io.ReadAll(io.LimitReader(conn, magicLen))
	if err != nil {
		return nil, nil, fmt.Errorf("peek network stream: %w", err)
	}

	if h, ok := m.handshakers[string(peeked)]; ok {
		return h.Handshake(conn, authInfo)
	}

	return &restoredConn{
		Conn:   conn,
		reader: io.MultiReader(bytes.NewReader(peeked), conn),
	}, authInfo, nil
}
