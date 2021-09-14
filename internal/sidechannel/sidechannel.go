package sidechannel

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"time"

	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

var magicBytes = []byte("sidechannel")

// sidechannelTimeout is the timeout for establishing a sidechannel
// connection. The sidechannel is supposed to be opened on the same wire with
// incoming grpc request. There won't be real handshaking involved, so it
// should be fast.
const (
	sidechannelTimeout     = 5 * time.Second
	sidechannelMetadataKey = "gitaly-sidechannel-id"
)

// OpenSidechannel opens a sidechannel connection from the stream opener
// extracted from the current peer connection.
func OpenSidechannel(ctx context.Context) (_ *ServerConn, err error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, fmt.Errorf("sidechannel: failed to extract incoming metadata")
	}
	ids := md.Get(sidechannelMetadataKey)
	if len(ids) == 0 {
		return nil, fmt.Errorf("sidechannel: sidechannel-id not found in incoming metadata")
	}
	sidechannelID, _ := strconv.ParseInt(ids[len(ids)-1], 10, 64)

	muxSession, err := backchannel.GetYamuxSession(ctx)
	if err != nil {
		return nil, fmt.Errorf("sidechannel: fail to extract yamux session: %w", err)
	}

	stream, err := muxSession.Open()
	if err != nil {
		return nil, fmt.Errorf("sidechannel: open stream: %w", err)
	}
	defer func() {
		if err != nil {
			stream.Close()
		}
	}()

	if err := stream.SetDeadline(time.Now().Add(sidechannelTimeout)); err != nil {
		return nil, err
	}

	if _, err := stream.Write(magicBytes); err != nil {
		return nil, fmt.Errorf("sidechannel: write magic bytes: %w", err)
	}

	if err := binary.Write(stream, binary.BigEndian, sidechannelID); err != nil {
		return nil, fmt.Errorf("sidechannel: write stream id: %w", err)
	}

	if err := stream.SetDeadline(time.Time{}); err != nil {
		return nil, err
	}

	return newServerConn(stream), nil
}

// RegisterSidechannel registers the caller into the waiting list of the
// sidechannel registry and injects the sidechannel ID into outgoing metadata.
// The caller is expected to establish the request with the returned context. The
// callback is executed automatically when the sidechannel connection arrives.
// The result is pushed to the error channel of the returned waiter.
func RegisterSidechannel(ctx context.Context, registry *Registry, callback func(*ClientConn) error) (context.Context, *Waiter) {
	waiter := registry.Register(callback)
	ctxOut := metadata.AppendToOutgoingContext(ctx, sidechannelMetadataKey, fmt.Sprintf("%d", waiter.id))
	return ctxOut, waiter
}

// ServerHandshaker implements the server-side sidechannel handshake.
type ServerHandshaker struct {
	registry *Registry
}

// Magic returns the magic bytes for sidechannel
func (s *ServerHandshaker) Magic() string {
	return string(magicBytes)
}

// Handshake implements the handshaking logic for sidechannel so that
// this handshaker reads the sidechannel ID from the wire, and then delegates
// the connection to the sidechannel registry
func (s *ServerHandshaker) Handshake(conn net.Conn, authInfo credentials.AuthInfo) (net.Conn, credentials.AuthInfo, error) {
	var sidechannelID sidechannelID
	if err := binary.Read(conn, binary.BigEndian, &sidechannelID); err != nil {
		return nil, nil, fmt.Errorf("sidechannel: fail to extract sidechannel ID: %w", err)
	}

	if err := s.registry.receive(sidechannelID, conn); err != nil {
		return nil, nil, err
	}

	// credentials.ErrConnDispatched, indicating that the connection is already
	// dispatched out of gRPC. gRPC should leave it alone and exit in peace.
	return nil, nil, credentials.ErrConnDispatched
}

// NewServerHandshaker creates a new handshaker for sidechannel to
// embed into listenmux.
func NewServerHandshaker(registry *Registry) *ServerHandshaker {
	return &ServerHandshaker{registry: registry}
}
