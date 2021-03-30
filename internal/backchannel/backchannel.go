// Package backchannel implements connection multiplexing that allows for invoking
// gRPC methods from the server to the client.
//
// gRPC allows only for invoking RPCs from client to the server. Invoking
// RPCs from the server to the client can be useful in some cases such as
// tunneling through firewalls. While implementing such a use case would be
// possible with plain bidirectional streams, the approach has various limitations
// that force additional work on the user. All messages in a single stream are ordered
// and processed sequentially. If concurrency is desired, this would require the user
// to implement their own concurrency handling. Request routing and cancellations would also
// have to be implemented separately on top of the bidirectional stream.
//
// To do away with these problems, this package provides a multiplexed transport for running two
// independent gRPC sessions on a single connection. This allows for dialing back to the client from
// the server to establish another gRPC session where the server and client roles are switched.
//
// The server side supports clients that are unaware of the multiplexing. The server peeks the incoming
// network stream to see if it starts with the magic bytes that indicate a multiplexing aware client.
// If the magic bytes are present, the server initiates the multiplexing session and dials back to the client
// over the already established network connection. If the magic bytes are not present, the server restores the
// the bytes back into the original network stream and handles it without a multiplexing session.
//
// Usage:
// 1. Implement a ServerFactory, which is simply a function that returns a Server that can serve on the backchannel
//    connection. Plug in the ClientHandshake returned by the ServerFactory.ClientHandshaker via grpc.WithTransportCredentials.
//    This ensures all connections established by gRPC work with a multiplexing session and have a backchannel Server serving.
// 2. Configure the ServerHandshake on the server side by passing it into the gRPC server via the grpc.Creds option.
//    The ServerHandshake method is called on each newly established connection. It peeks the network stream to see if a
//    multiplexing session should be initiated. If so, it also dials back to the client's backchannel server. Server
//    makes the backchannel connection's available later via the Registry's Backchannel method. The ID of the
//    peer associated with the current RPC handler can be fetched via GetPeerID. The returned ID can be used
//    to access the correct backchannel connection from the Registry.
package backchannel

import (
	"io"
	"net"

	"github.com/hashicorp/yamux"
)

// magicBytes are sent by the client to server to identify as a multiplexing aware client.
var magicBytes = []byte("backchannel")

// muxConfig returns a new config to use with the multiplexing session.
func muxConfig(logger io.Writer) *yamux.Config {
	cfg := yamux.DefaultConfig()
	cfg.LogOutput = logger
	return cfg
}

// connCloser wraps a net.Conn and calls the provided close function instead when Close
// is called.
type connCloser struct {
	net.Conn
	close func() error
}

// Close calls the provided close function.
func (cc connCloser) Close() error { return cc.close() }
