package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

func main() {
	gitalyLn, err := net.Listen("tcp", "localhost:8080")
	if err != nil {
		log.Fatalf("listen: %q", err)
	}

	var m sync.Mutex

	persistentStreamsByPeer := map[string]chan<- func(){}
	gitalySrv := grpc.NewServer()
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

						log.Printf("RelayVotes: persistent stream opened for %q", peerInfo.Addr)

						votes := make(chan func())

						m.Lock()
						persistentStreamsByPeer[peerInfo.Addr.String()] = votes
						m.Unlock()

						log.Printf("RelayVotes: relaying votes to %q", peerInfo.Addr.String())
						for responseFunc := range votes {
							log.Printf("RelayVotes: relaying a vote to %q", peerInfo.Addr)
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
						log.Printf("TransactionalMutator: received mutator from: %q", peerInfo.Addr)

						if err := stream.SendMsg(&gitalypb.VoteTransactionResponse{}); err != nil {
							log.Fatalf("SendMsg: %q", err)
						}

						// This part emulates the RPC invoking the hooks, which calls the HookService's
						// ReferenceTransaction method.
						hookServiceClient, err := grpc.Dial(gitalyLn.Addr().String(), grpc.WithInsecure())
						if err != nil {
							log.Fatalf("dial hook service: %q", err)
						}
						hookServiceClient.Invoke(
							metadata.NewOutgoingContext(stream.Context(), metadata.New(map[string]string{"praefect-peer": peerInfo.Addr.String()})),
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
						log.Printf("ReferenceTransaction: actual peer was  : %q", peerInfo.Addr)

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

	// Dial the gitaly nodes from each Praefect. The Proxied RPCs and the persistent vote relaying
	// stream would use the same connection.
	var praefects []*grpc.ClientConn
	for i := 0; i < 3; i++ {
		cc, err := grpc.Dial(gitalyLn.Addr().String(), grpc.WithInsecure())
		if err != nil {
			log.Fatalf("dial: %q", err)
		}

		praefects = append(praefects, cc)
	}

	ctx := context.Background()

	// set up the persistent streams for relaying votes from Gitalys to Praefect. Praefect dialed the
	// connection, so it also initiates the stream.
	for i, praefect := range praefects {
		i := i
		praefect := praefect
		go func() {
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

			for {
				// Praefect receives a vote on the stream relayed by Gitaly.
				if err := stream.RecvMsg(&gitalypb.VoteTransactionResponse{}); err != nil {
					log.Fatalf("error receiving vote: %q", err)
				}
				log.Printf("Praefect %d received vote", i)
				log.Printf("Praefect %d responding to vote", i)

				// Praefect responds to the vote.
				if err := stream.SendMsg(&gitalypb.VoteTransactionResponse{}); err != nil {
					log.Fatalf("error sending vote response: %q", err)
				}
			}
		}()
	}

	// wait for the streams to be opened in a simple manner
	time.Sleep(time.Second)

	// Call mutator RPCs from each 'praefect' one after another
	for request := 0; request < 2; request++ {
		for i, praefect := range praefects {
			fmt.Println("")
			log.Printf("Praefect %d calling a mutator RPC", i)
			if err := praefect.Invoke(context.Background(), "/Gitaly/TransactionalMutator", &gitalypb.VoteTransactionRequest{}, &gitalypb.VoteTransactionResponse{}); err != nil {
				log.Fatalf("invoke: %q", err)
			}
		}
	}
}
