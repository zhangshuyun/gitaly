package streamrpcs

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"time"

	"gitlab.com/gitlab-org/gitaly/v14/internal/bootstrap/starter"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const sidechannelIdentityKey = "Gitaly-SideChannel-Identity"
const defaultSidechannelTimeout = 10 * time.Second
const maxClientRetryAttempts = 3

// DialFunc is a method making a secondary sidechannel connection to a
// token-proteted address
type DialFunc func(time.Time, string, string) (net.Conn, error)

func identity(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	values := md.Get(sidechannelIdentityKey)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

// DialNet lets Call initiate unencrypted connections. They tend to be used
// with Gitaly's listenmux multiplexer only. After the connection is
// established, streamrpc's 11-byte magic bytes are written into the wire.
// Listemmux peeks into these magic bytes and redirects the request to
// StreamRPC server.
// Please visit internal/listenmux/mux.go for more information
func DialNet() DialFunc {
	return func(deadline time.Time, address string, token string) (net.Conn, error) {
		endpoint, err := starter.ParseEndpoint(address)
		if err != nil {
			return nil, err
		}

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
			return nil, fmt.Errorf("streamrpc client: write magic bytes: %w", err)
		}

		// Write the stream token into the wire. This token lets the server
		// matches waiting RPC handler
		if _, err := conn.Write([]byte(token)); err != nil {
			return nil, fmt.Errorf("streamrpc client: write stream token: %w", err)
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
func DialTLS(cfg *tls.Config) DialFunc {
	return func(deadline time.Time, address string, token string) (net.Conn, error) {
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

		// Write the stream token into the wire. This token lets the server
		// matches waiting RPC handler
		if _, err := tlsConn.Write([]byte(token)); err != nil {
			return nil, fmt.Errorf("streamrpc client: write stream token: %w", err)
		}

		// Reset deadline of tls connection for later stages
		if err = tlsConn.SetDeadline(time.Time{}); err != nil {
			return nil, err
		}

		return tlsConn, nil
	}
}

// Call enables the client to make gRPC calls to the server. While handling the
// RPC call, a sidechannel TCP connection is established between client and
// server over the same listening gRPC port. This allows the clients and
// servers exchange information over the raw TCP connection without the
// overhead of gRPC. This consists of some steps:
//
// - Client preares the client stream via `handshake` func. It may send as many
// requests to the server as it wants. A typical use case is to send repository
// information for validation beforehand.
// - After the `handshake` func exits, this method waits for StreamToken
// response from the server.
// - This method establishes a sidechannel TCP connection to gRPC server. This
// can be done thanks to listenmux multiplexer.
// - The raw connection is given back to client `handler` func for further data
// exchange.
// - This method waits until the stream is closed.
//
// As we are making two sequential calls with sub small steps, a lot of things
// may happen. One notable case is that the secondary dial may fail during
// deployment when a new Gitaly process is spawn. Therefore we should retry
// multiple times if any step fails.
func Call(ctx context.Context, addr string, handshake func(context.Context) (grpc.ClientStream, error), dial DialFunc, handler func(net.Conn) error) (finalError error) {
	doCall := func() (err error) {
		var stream grpc.ClientStream
		var streamToken gitalypb.StreamToken

		// Make the first call(s). Let the caller preare the request data.
		ctx = metadata.AppendToOutgoingContext(ctx, sidechannelIdentityKey, addr)
		if stream, err = handshake(ctx); err != nil {
			return err
		}

		// We don't need to send any further information
		if err = stream.CloseSend(); err != nil {
			return err
		}

		// Wait for stream token from the server
		if err = stream.RecvMsg(&streamToken); err != nil {
			return err
		}

		// Make the secondary call to the same address, with the token received
		// from the server
		deadline, ok := ctx.Deadline()
		if !ok {
			deadline = time.Now().Add(defaultSidechannelTimeout)
		}

		conn, err := dial(deadline, addr, streamToken.Token)
		if err != nil {
			return err
		}
		defer conn.Close()

		// Delegate the raw connection to the caller
		if err = handler(conn); err != nil {
			return err
		}

		// The server should return, and close the streaming RPC.
		err = stream.RecvMsg(&streamToken)
		if err == nil {
			return fmt.Errorf("streamrpc client: expected server stream closed")
		} else if err == io.EOF {
			return nil
		} else {
			return err
		}
	}
	for i := 0; i < maxClientRetryAttempts; i++ {
		finalError = doCall()
		if finalError == nil {
			break
		}
	}
	return finalError
}

// AcceptConnection blocks the RPC handlers until the sidechannel TCP connection arrives.
func AcceptConnection(ctx context.Context, stream grpc.ServerStream) (net.Conn, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(defaultSidechannelTimeout)
	}

	waiter, err := globalRegistry.Register(deadline)
	if err != nil {
		return nil, err
	}

	err = stream.SendMsg(&gitalypb.StreamToken{
		Cookie: identity(ctx),
		Token:  waiter.Token,
	})
	if err != nil {
		return nil, err
	}

	return waiter.Wait()
}
