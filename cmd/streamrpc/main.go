package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/hashicorp/yamux"
	"gitlab.com/gitlab-org/gitaly/v14/internal/listenmux"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

func main() {
	server := flag.Bool("server", false, "take the server role")
	flag.Parse()

	if *server {
		gitaly()
	} else {
		client()
	}
}

// Registry is the glue between RPC invocations waiting to receive sidechannel
// data and the listener accepting new sidechannel connections.
type registry struct {
	nextID  int64
	waiters map[int64]func(io.Reader)
}

// Await registers a new waiter that should be invoked when the sidechannel
// data is available. It returns a sidechannel id the client sents to the server
// which the server back when opening the sidechannel. The sidechannel id allows
// for correlating the accepted sidechannel streams to the callbacks waiting for them.
func (r *registry) Await(callback func(r io.Reader)) (int64, func()) {
	id := r.nextID
	r.nextID++
	r.waiters[id] = callback
	return id, func() { delete(r.waiters, id) }
}

// Listen waits accepts sidechannel connections from the server. The server sends
// the sidechannel's id as the first thing on the stream which allows the client to
// match the sidechannel data that the server is about to send with one of the
// registered callbacks.
func (r *registry) Listen(ln *yamux.Session) error {
	for {
		sidechannel, err := ln.Accept()
		if err != nil {
			return fmt.Errorf("accept: %w", err)
		}

		var id int64
		if err := binary.Read(sidechannel, binary.BigEndian, &id); err != nil {
			return fmt.Errorf("read sidechannel id: %w", err)
		}

		callback, ok := r.waiters[id]
		if !ok {
			return fmt.Errorf("invalid sidechannel id: %v", id)
		}

		log.Printf("received id: %d", id)
		callback(sidechannel)
	}
}

type clientHandshake struct {
	credentials.TransportCredentials
	*registry
}

// ClientHandshake opens one stream for the gRPC session and starts listening for
// incoming sidechannel connections.
func (ch clientHandshake) ClientHandshake(ctx context.Context, serverName string, conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn, authInfo, err := ch.TransportCredentials.ClientHandshake(ctx, serverName, conn)
	if err != nil {
		log.Fatal("original client handshake: %w", err)
	}

	_, err = conn.Write([]byte("mux00000000"))
	if err != nil {
		log.Fatal("write grpc stream magic: %w", err)
	}

	session, err := yamux.Client(conn, nil)
	if err != nil {
		log.Fatal("yamux client: %w", err)
	}

	// First open the client's gRPC stream to the server.
	grpcStream, err := session.Open()
	if err != nil {
		log.Fatal("open grpc stream: %w", err)
	}

	// Here we begin listening for the incoming sidechannel connections.
	go func() {
		if err := ch.registry.Listen(session); err != nil {
			log.Fatalf("listen for sidechannels: %v", err)
		}
	}()

	return grpcStream, authInfo, nil
}

// client is the client side of the sidechannel protocol.
func client() {
	registry := &registry{waiters: make(map[int64]func(io.Reader))}
	clientConn, err := grpc.Dial("localhost:8080", grpc.WithTransportCredentials(
		clientHandshake{
			TransportCredentials: insecure.NewCredentials(),
			registry:             registry,
		},
	))
	if err != nil {
		log.Fatal("dial server: %w", err)
	}

	for {
		// Each invocation of this func is a separate rpc call
		func() {
			// Before calling the Gitaly, we set up a callback that
			// will handle the sidechannel data that the server sends.
			// This also returns the stream is the server should send back to
			// us so we can correlate this await call to the sidechannel
			// that is being opened.
			streamID, clean := registry.Await(func(r io.Reader) {
				sidechannelData, err := io.ReadAll(r)
				if err != nil {
					log.Println("read sidechannel data: %w", err)
				}

				log.Printf("received sidechannel data: %q", sidechannelData)
			})
			defer clean()

			// here we just send hte server the sidechannel's id it should send back to us.
			ctx := metadata.NewOutgoingContext(context.Background(), metadata.New(map[string]string{
				"sidechannel-id": fmt.Sprintf("%d", streamID),
			}))

			// perform the RPC call
			if err := clientConn.Invoke(ctx, "/Gitaly/Mutator", &gitalypb.CreateBranchRequest{}, &gitalypb.CreateBranchResponse{}); err != nil {
				log.Fatalf("call server: %v", err)
			}
			fmt.Println()
			time.Sleep(time.Second)
		}()
	}

}

// gitaly is the server side of the sidechannel implementation.
func gitaly() {
	ln, err := net.Listen("tcp", "localhost:8080")
	if err != nil {
		log.Fatal("listen: %w", err)
	}

	// here we use listenmux to have a multiplexed transport easily open
	lnMux := listenmux.New(insecure.NewCredentials())
	lnMux.Register(muxedServer{})

	if err := grpc.NewServer(grpc.Creds(lnMux), grpc.UnknownServiceHandler(func(srv interface{}, stream grpc.ServerStream) error {
		fmt.Println("Gitaly received RPC")

		// We use the AuthInfo to pass the RPC handler a streamOpener it can
		// use to open a sidechannel back to the client.
		peerInfo, ok := peer.FromContext(stream.Context())
		if !ok {
			return errors.New("no peer info in context")
		}

		streamOpener, ok := peerInfo.AuthInfo.(interface {
			OpenSidechannel(context.Context) (io.WriteCloser, error)
		})
		if !ok {
			return fmt.Errorf("not a stream opener: %T", peerInfo.AuthInfo)
		}

		// Here we open the sidechannel to the client
		fmt.Println("Gitaly opening sidechannel")
		sidechannel, err := streamOpener.OpenSidechannel(stream.Context())
		if err != nil {
			return fmt.Errorf("open stream: %w", err)
		}

		// With the channel open, we can write data to it
		fmt.Println("Gitaly writing to sidechannel")
		_, err = sidechannel.Write([]byte("data sent over sidechannel"))
		// we close the channel so the client knows we are done writing.
		sidechannel.Close()
		if err != nil {
			return fmt.Errorf("write to sidechannel: %w", err)
		}

		// Send a gRPC response back to the client.
		fmt.Println("Gitaly responding to RPC")
		return stream.SendMsg(&gitalypb.CreateBranchResponse{})
	})).Serve(ln); err != nil {
		log.Fatal("serve: %w")
	}
}

// streamOpener is injected via the AuthInfo in the context to the RPC handlers
// on the server.
type streamOpener struct {
	credentials.AuthInfo
	session *yamux.Session
}

// OpenSidechannel opens a new sidechannel to the client.
func (opener *streamOpener) OpenSidechannel(ctx context.Context) (io.WriteCloser, error) {
	// First step is to extract the sidechannel id the client sent us so
	// we can send it back. The ID is how the client correlates this particular sidechannel
	// to a specific RPC invocation on its side.
	md, _ := metadata.FromIncomingContext(ctx)
	id, _ := strconv.ParseInt(md["sidechannel-id"][0], 10, 64)
	log.Printf("opening sidechannel id %d", id)

	// open a new stream in the multiplexing session
	stream, err := opener.session.Open()
	if err != nil {
		return nil, fmt.Errorf("open stream: %w", err)
	}

	// write the sidechannel id so the client can correlate this stream to an RPC.
	if err := binary.Write(stream, binary.BigEndian, id); err != nil {
		stream.Close()
		return nil, fmt.Errorf("write stream id: %w", err)
	}

	// sidechannel is ready for use.
	return stream, nil
}

type muxedServer struct{}

// The handshake just opens a multiplexing session on top of the network connection
// that we received from the client.
func (muxedServer) Handshake(conn net.Conn, authInfo credentials.AuthInfo) (net.Conn, credentials.AuthInfo, error) {
	session, err := yamux.Server(conn, nil)
	if err != nil {
		log.Fatal("yamux server: %w", err)
	}

	clientToServerGRPC, err := session.Accept()
	if err != nil {
		log.Fatal("accept grpc stream: %w", err)
	}

	return clientToServerGRPC, &streamOpener{
		AuthInfo: authInfo, session: session,
	}, nil
}

func (muxedServer) Magic() string { return "mux00000000" }
