package backchannel_test

import (
	"context"
	"fmt"
	"net"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func Example() {
	// Open the server's listener.
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		fmt.Printf("failed to start listener: %v", err)
		return
	}

	// Registry is for storing the open backchannels. It should be passed into the ServerHandshaker
	// which creates the backchannel connections and adds them to the registry. The RPC handlers
	// can use the registry to access available backchannels by their peer ID.
	registry := backchannel.NewRegistry()

	logger := logrus.NewEntry(logrus.New())

	// ServerHandshaker initiates the multiplexing session on the server side. Once that is done,
	// it creates the backchannel connection and stores it into the registry. For each connection,
	// the ServerHandshaker passes down the peer ID via the context. The peer ID identifies a
	// backchannel connection.
	handshaker := backchannel.NewServerHandshaker(logger, backchannel.Insecure(), registry, nil)

	// Create the server
	srv := grpc.NewServer(
		grpc.Creds(handshaker),
		grpc.UnknownServiceHandler(func(srv interface{}, stream grpc.ServerStream) error {
			fmt.Println("Gitaly received a transactional mutator")

			backchannelID, err := backchannel.GetPeerID(stream.Context())
			if err == backchannel.ErrNonMultiplexedConnection {
				// This call is from a client that is not multiplexing aware. Client is not
				// Praefect, so no need to perform voting. The client could be for example
				// GitLab calling Gitaly directly.
				fmt.Println("Gitaly responding to a non-multiplexed client")
				return stream.SendMsg(&gitalypb.CreateBranchResponse{})
			} else if err != nil {
				return fmt.Errorf("get peer id: %w", err)
			}

			backchannelConn, err := registry.Backchannel(backchannelID)
			if err != nil {
				return fmt.Errorf("get backchannel: %w", err)
			}

			fmt.Println("Gitaly sending vote to Praefect via backchannel")
			if err := backchannelConn.Invoke(
				stream.Context(), "/Praefect/VoteTransaction",
				&gitalypb.VoteTransactionRequest{}, &gitalypb.VoteTransactionResponse{},
			); err != nil {
				return fmt.Errorf("invoke backchannel: %w", err)
			}
			fmt.Println("Gitaly received vote response via backchannel")

			fmt.Println("Gitaly responding to the transactional mutator")
			return stream.SendMsg(&gitalypb.CreateBranchResponse{})
		}),
	)
	defer srv.Stop()

	// Start the server
	go func() {
		if err := srv.Serve(ln); err != nil {
			fmt.Printf("failed to serve: %v", err)
		}
	}()

	fmt.Printf("Invoke with a multiplexed client:\n\n")
	if err := invokeWithMuxedClient(logger, ln.Addr().String()); err != nil {
		fmt.Printf("failed to invoke with muxed client: %v", err)
		return
	}

	fmt.Printf("\nInvoke with a non-multiplexed client:\n\n")
	if err := invokeWithNormalClient(ln.Addr().String()); err != nil {
		fmt.Printf("failed to invoke with non-muxed client: %v", err)
		return
	}
	// Output:
	// Invoke with a multiplexed client:
	//
	// Gitaly received a transactional mutator
	// Gitaly sending vote to Praefect via backchannel
	// Praefect received vote via backchannel
	// Praefect responding via backchannel
	// Gitaly received vote response via backchannel
	// Gitaly responding to the transactional mutator
	//
	// Invoke with a non-multiplexed client:
	//
	// Gitaly received a transactional mutator
	// Gitaly responding to a non-multiplexed client
}

func invokeWithMuxedClient(logger *logrus.Entry, address string) error {
	// clientHandshaker's ClientHandshake gets called on each established connection. The Server returned by the
	// ServerFactory is started on Praefect's end of the connection, which Gitaly can call.
	clientHandshaker := backchannel.NewClientHandshaker(logger, func() backchannel.Server {
		return grpc.NewServer(grpc.UnknownServiceHandler(func(srv interface{}, stream grpc.ServerStream) error {
			fmt.Println("Praefect received vote via backchannel")
			fmt.Println("Praefect responding via backchannel")
			return stream.SendMsg(&gitalypb.VoteTransactionResponse{})
		}))
	})

	return invokeWithOpts(address, grpc.WithTransportCredentials(clientHandshaker.ClientHandshake(backchannel.Insecure())))
}

func invokeWithNormalClient(address string) error {
	return invokeWithOpts(address, grpc.WithInsecure())
}

func invokeWithOpts(address string, opts ...grpc.DialOption) error {
	clientConn, err := grpc.Dial(address, opts...)
	if err != nil {
		return fmt.Errorf("dial server: %w", err)
	}

	if err := clientConn.Invoke(context.Background(), "/Gitaly/Mutator", &gitalypb.CreateBranchRequest{}, &gitalypb.CreateBranchResponse{}); err != nil {
		return fmt.Errorf("call server: %w", err)
	}

	if err := clientConn.Close(); err != nil {
		return fmt.Errorf("close clientConn: %w", err)
	}

	return nil
}
