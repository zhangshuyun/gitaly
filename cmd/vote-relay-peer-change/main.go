package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

type transportCredentials struct {
	credentials.TransportCredentials
	connectionID int64
}

type authInfo struct{ connectionID int64 }

func (authInfo) AuthType() string { return "connection identifier" }

func (tc *transportCredentials) ServerHandshake(conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	// We log here new connections that get established to the Gitaly. In this case, we're also using
	// this to give an arbitary ID
	connID := atomic.AddInt64(&tc.connectionID, 1)
	log.Printf("Connection %d established to Gitaly", connID)
	return conn, authInfo{connID}, nil
}

func main() {
	unix := flag.Bool("unix", false, "use a unix socket")
	flag.Parse()

	listenFunc := func() (net.Listener, error) { return net.Listen("tcp", "localhost:8080") }
	if *unix {
		listenFunc = func() (net.Listener, error) { return net.Listen("unix", "gitaly-socket") }
		defer os.Remove("gitaly-socket")
	}

	gitalyLn, err := listenFunc()
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	// we can key the connections with their network scheme to support both unix and tcp connections
	peerKey := func(p *peer.Peer) string {
		return fmt.Sprintf("%d", p.AuthInfo.(authInfo).connectionID)
		switch p.Addr.Network() {
		case "tcp":
			return fmt.Sprintf("tcp://%s", p.Addr)
		case "unix":
			return fmt.Sprintf("unix://%p", p.Addr)
		}

		return p.Addr.Network() + ":" + fmt.Sprintf("%p", p.Addr)
	}

	// dialer is just for convenience here so we don't have handle the difference in dialing
	// the two different schemes

	dialer := func(capture *net.Conn) grpc.DialOption {
		return grpc.WithDialer(func(string, time.Duration) (net.Conn, error) {
			conn, err := net.Dial(gitalyLn.Addr().Network(), gitalyLn.Addr().String())
			*capture = conn
			return conn, err
		})
	}

	var m sync.Mutex

	// we are using transport credentials here to get access to the connection handle
	// on the Gitaly server. We'll close the connection after the first RPC has concluded
	tc := &transportCredentials{}

	persistentStreamsByPeer := map[string]chan<- func(){}
	gitalySrv := grpc.NewServer(grpc.Creds(tc))
	gitalySrv.RegisterService(
		&grpc.ServiceDesc{
			ServiceName: "Gitaly",
			Streams: []grpc.StreamDesc{
				// RelayVotes is the persistent stream initiated from Praefect to Gitaly.
				// Gitaly relays votes to the same Praefect that the transactional mutator
				// originated from.
				//
				// The handler stores the open streams in to map HookService's ReferenceTransaction
				// can access them to relay the votes.
				{
					StreamName:    "RelayVotes",
					ClientStreams: true,
					ServerStreams: true,
					Handler: func(srv interface{}, stream grpc.ServerStream) error {
						// Peer is the Praefect node we have an open stream to.
						peerInfo, _ := peer.FromContext(stream.Context())
						log.Printf("RelayVotes: persistent stream opened for connection id: %d", peerInfo.AuthInfo.(authInfo).connectionID)

						votes := make(chan func())

						m.Lock()
						persistentStreamsByPeer[peerKey(peerInfo)] = votes
						m.Unlock()

						log.Printf("RelayVotes: relaying votes to %q", peerKey(peerInfo))
						for responseFunc := range votes {
							log.Printf("RelayVotes: relaying a vote to %q", peerKey(peerInfo))
							if err := stream.SendMsg(&gitalypb.VoteTransactionRequest{}); err != nil {
								log.Fatalf("relay vote to praefect: %q", err)
							}

							if err := stream.RecvMsg(&gitalypb.VoteTransactionResponse{}); err != nil {
								log.Fatalf("receive response from praefect: %q", err)
							}

							responseFunc()
						}
						return nil
					},
				},
				// TranscationalMutator is a mutator RPC that needs to cast a vote. It does so by
				// calling the HookService's ReferenceTransaction. It passes the Praefect peer's
				// ID to ReferenceTransaction so it knows where to route the vote.
				{
					StreamName:    "TransactionalMutator",
					ClientStreams: true,
					ServerStreams: true,
					Handler: func(srv interface{}, stream grpc.ServerStream) error {
						// Peer is the Praefect node routing the request and the same Praefect
						// that needs to receive the vote.
						peerInfo, _ := peer.FromContext(stream.Context())
						log.Printf("TransactionalMutator: received mutator from: %q", peerKey(peerInfo))

						if err := stream.SendMsg(&gitalypb.VoteTransactionResponse{}); err != nil {
							log.Fatalf("SendMsg: %q", err)
						}

						// This part emulates the RPC invoking the hooks, which calls the HookService's
						// ReferenceTransaction method.
						var conn net.Conn
						hookServiceClient, err := grpc.Dial("", dialer(&conn), grpc.WithInsecure())
						if err != nil {
							log.Fatalf("dial hook service: %q", err)
						}
						hookServiceClient.Invoke(
							metadata.NewOutgoingContext(stream.Context(), metadata.New(map[string]string{"praefect-peer": peerKey(peerInfo)})),
							"/Gitaly/ReferenceTransaction", &gitalypb.VoteTransactionRequest{}, &gitalypb.VoteTransactionResponse{},
						)

						return nil
					},
				},
				// ReferenceTransaction is the HookService's method where the votes are sent from. It takes the
				// information of the peer Praefect of the mutator and sends the vote to the same Praefect.
				{
					StreamName:    "ReferenceTransaction",
					ClientStreams: true,
					ServerStreams: true,
					Handler: func(srv interface{}, stream grpc.ServerStream) error {
						// Peer is the `gitaly-hooks` calling the HookService. Peer is not the Praefect
						// so we need to use the passed through info to access the stream.
						peerInfo, _ := peer.FromContext(stream.Context())
						log.Printf("ReferenceTransaction: actual peer was: %q", peerKey(peerInfo))

						md, _ := metadata.FromIncomingContext(stream.Context())
						praefectPeer := md["praefect-peer"][0]

						log.Printf("ReferenceTransaction: praefect peer was: %q", praefectPeer)

						log.Printf("ReferenceTransaction: sending vote")

						voteReceived := make(chan struct{})
						persistentStreamsByPeer[praefectPeer] <- func() {
							close(voteReceived)
						}
						<-voteReceived
						log.Printf("ReferenceTransaction: response to vote received")

						return nil
					},
				},
			},
			HandlerType: (*interface{})(nil),
		},
		struct{}{},
	)

	go func() { gitalySrv.Serve(gitalyLn) }()

	// we capture the connection from the dialer so we can close it later to simulate
	// connection loss
	var capturedClientConn net.Conn

	// Dial the Gitaly nodes from the Praefect. The Proxied RPCs and the persistent vote relaying
	// share the same connection.
	praefect, err := grpc.Dial("", dialer(&capturedClientConn), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("error dialing gitaly from praefect: %q", err)
	}

	ctx := context.Background()

	// We set up the persistent stream for vote relaying here. The stream within a loop in order
	// to re-establish the stream after a connection loss.
	go func() {
		for {
			stream, err := praefect.NewStream(
				ctx,
				&grpc.StreamDesc{
					ClientStreams: true,
					ServerStreams: true,
				},
				"/Gitaly/RelayVotes",
			)
			if err != nil {
				log.Fatalf("persistent stream: %q", err)
			}
			log.Printf("Praefect established persistent stream to Gitaly")

			for {
				// Praefect receives a vote on the stream relayed by Gitaly.
				if err := stream.RecvMsg(&gitalypb.VoteTransactionResponse{}); err != nil {
					log.Printf("PersistentStream: error receiving vote: %v", err)
					break
				}

				log.Printf("Praefect received vote")
				log.Printf("Praefect responding to vote")

				// Praefect responds to the vote.
				if err := stream.SendMsg(&gitalypb.VoteTransactionResponse{}); err != nil {
					log.Printf("PersistentStream: error sending vote response: %v", err)
					break
				}
			}
		}
	}()

	// wait for the streams to be opened in a simple manner
	time.Sleep(time.Second)

	// Call the first mutator RPC
	fmt.Println("")
	log.Printf("Praefect calling first mutator RPC")
	if err := praefect.Invoke(context.Background(), "/Gitaly/TransactionalMutator", &gitalypb.VoteTransactionRequest{}, &gitalypb.VoteTransactionResponse{}); err != nil {
		log.Fatalf("invoke first: %q", err)
	}

	// Here we simulate a connection loss by closing the connection behind the *grpc.ClientConn
	fmt.Println("")
	log.Printf("Praefect's connection to Gitaly breaking")
	if err := capturedClientConn.Close(); err != nil {
		log.Fatalf("close capture client conn: %v", err)
	}

	// Call the second mutator RPC. gRPC has transparently reconnected to the Gitaly.
	fmt.Println("")
	log.Print("Praefect calling second mutator RPC")
	if err := praefect.Invoke(context.Background(), "/Gitaly/TransactionalMutator", &gitalypb.VoteTransactionRequest{}, &gitalypb.VoteTransactionResponse{}); err != nil {
		log.Fatalf("invoke second: %q", err)
	}
}
