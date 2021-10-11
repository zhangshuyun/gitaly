package sidechannel

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/hashicorp/yamux"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/listenmux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
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

type options struct {
	yamuxConfig *yamux.Config
}

func defaultSidechannelOptions(logger io.Writer) *options {
	yamuxConf := yamux.DefaultConfig()

	// At the moment, those configurations are the subset of backchannel yamux
	// configurations, defined in internal/backchannel/backchannel.go. It's
	// subject to change in the near future.
	yamuxConf.MaxStreamWindowSize = 16 * 1024 * 1024
	yamuxConf.EnableKeepAlive = false
	yamuxConf.LogOutput = logger

	return &options{
		yamuxConfig: yamuxConf,
	}
}

// A Option sets options such as yamux configurations for sidechannel
type Option func(*options)

// WithYamuxConfig customizes the yamux configuration used in sidechannel
func WithYamuxConfig(yamuxConfig *yamux.Config) Option {
	return func(opts *options) { opts.yamuxConfig = yamuxConfig }
}

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

	buf := make([]byte, 2)
	if _, err := io.ReadFull(stream, buf); err != nil {
		return nil, fmt.Errorf("sidechannel: receive confirmation: %w", err)
	}
	if string(buf) != "ok" {
		return nil, fmt.Errorf("sidechannel: expected ok, got %q", buf)
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

// NewClientHandshaker is used to enable sidechannel support on outbound
// gRPC connections.
func NewClientHandshaker(logger *logrus.Entry, registry *Registry, opts ...Option) client.Handshaker {
	sidechannelOpts := defaultSidechannelOptions(logger.Logger.Out)
	for _, opt := range opts {
		opt(sidechannelOpts)
	}

	return backchannel.NewClientHandshaker(
		logger,
		func() backchannel.Server {
			lm := listenmux.New(insecure.NewCredentials())
			lm.Register(NewServerHandshaker(registry))
			return grpc.NewServer(grpc.Creds(lm))
		},
		backchannel.WithYamuxConfig(sidechannelOpts.yamuxConfig),
	)
}
