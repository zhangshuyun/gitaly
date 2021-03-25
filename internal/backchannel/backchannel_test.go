package backchannel

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockTransactionServer struct {
	voteTransactionFunc func(context.Context, *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error)
	*gitalypb.UnimplementedRefTransactionServer
}

func (m mockTransactionServer) VoteTransaction(ctx context.Context, req *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
	return m.voteTransactionFunc(ctx, req)
}

func TestBackchannel_concurrentRequestsFromMultipleClients(t *testing.T) {
	registry := NewRegistry()
	handshaker := NewServerHandshaker(testhelper.DiscardTestEntry(t), Insecure(), registry)

	ln, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	errNonMultiplexed := status.Error(codes.FailedPrecondition, ErrNonMultiplexedConnection.Error())
	srv := grpc.NewServer(grpc.Creds(handshaker))

	gitalypb.RegisterRefTransactionServer(srv, mockTransactionServer{
		voteTransactionFunc: func(ctx context.Context, req *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
			peerID, err := GetPeerID(ctx)
			if err == ErrNonMultiplexedConnection {
				return nil, errNonMultiplexed
			}
			assert.NoError(t, err)

			cc, err := registry.Backchannel(peerID)
			if !assert.NoError(t, err) {
				return nil, err
			}

			return gitalypb.NewRefTransactionClient(cc).VoteTransaction(ctx, req)
		},
	})

	defer srv.Stop()
	go srv.Serve(ln)

	ctx, cancel := testhelper.Context()
	defer cancel()

	start := make(chan struct{})

	// Create 25 multiplexed clients and non-multiplexed clients that launch requests
	// concurrently.
	var wg sync.WaitGroup
	for i := uint64(0); i < 25; i++ {
		i := i
		wg.Add(2)

		go func() {
			defer wg.Done()

			<-start
			client, err := grpc.Dial(ln.Addr().String(), grpc.WithInsecure())
			if !assert.NoError(t, err) {
				return
			}

			resp, err := gitalypb.NewRefTransactionClient(client).VoteTransaction(ctx, &gitalypb.VoteTransactionRequest{})
			assert.Equal(t, err, errNonMultiplexed)
			assert.Nil(t, resp)

			assert.NoError(t, client.Close())
		}()

		go func() {
			defer wg.Done()

			expectedErr := status.Error(codes.Internal, fmt.Sprintf("multiplexed %d", i))
			serverFactory := ServerFactory(func() Server {
				srv := grpc.NewServer()
				gitalypb.RegisterRefTransactionServer(srv, mockTransactionServer{
					voteTransactionFunc: func(ctx context.Context, req *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
						assert.Equal(t, &gitalypb.VoteTransactionRequest{TransactionId: i}, req)
						return nil, expectedErr
					},
				})

				return srv
			})

			<-start
			client, err := grpc.Dial(ln.Addr().String(),
				grpc.WithTransportCredentials(serverFactory.ClientHandshaker(testhelper.DiscardTestEntry(t), Insecure())),
			)
			if !assert.NoError(t, err) {
				return
			}

			// Run two invocations concurrently on each multiplexed client to sanity check
			// the routing works with multiple requests from a connection.
			var invocations sync.WaitGroup
			for invocation := 0; invocation < 2; invocation++ {
				invocations.Add(1)
				go func() {
					defer invocations.Done()
					resp, err := gitalypb.NewRefTransactionClient(client).VoteTransaction(ctx, &gitalypb.VoteTransactionRequest{TransactionId: i})
					assert.Equal(t, err, expectedErr)
					assert.Nil(t, resp)
				}()
			}

			invocations.Wait()
			assert.NoError(t, client.Close())
		}()
	}

	// Establish the connection and fire the requests.
	close(start)

	// Wait for the clients to finish their calls and close their connections.
	wg.Wait()
}

type mockSSHService struct {
	sshUploadPackFunc func(gitalypb.SSHService_SSHUploadPackServer) error
	*gitalypb.UnimplementedSSHServiceServer
}

func (m mockSSHService) SSHUploadPack(stream gitalypb.SSHService_SSHUploadPackServer) error {
	return m.sshUploadPackFunc(stream)
}

func Benchmark(b *testing.B) {
	for _, tc := range []struct {
		desc        string
		multiplexed bool
	}{
		{desc: "multiplexed", multiplexed: true},
		{desc: "normal"},
	} {
		b.Run(tc.desc, func(b *testing.B) {
			for _, messageSize := range []int64{
				1024,
				1024 * 1024,
				3 * 1024 * 1024,
			} {
				b.Run(fmt.Sprintf("message size %dkb", messageSize/1024), func(b *testing.B) {
					var serverOpts []grpc.ServerOption
					if tc.multiplexed {
						serverOpts = []grpc.ServerOption{
							grpc.Creds(NewServerHandshaker(testhelper.DiscardTestEntry(b), Insecure(), NewRegistry())),
						}
					}

					srv := grpc.NewServer(serverOpts...)
					gitalypb.RegisterSSHServiceServer(srv, mockSSHService{
						sshUploadPackFunc: func(stream gitalypb.SSHService_SSHUploadPackServer) error {
							for {
								_, err := stream.Recv()
								if err != nil {
									assert.Equal(b, io.EOF, err)
									return nil
								}
							}
						},
					})

					ln, err := net.Listen("tcp", "localhost:0")
					require.NoError(b, err)

					defer srv.Stop()
					go srv.Serve(ln)

					ctx, cancel := testhelper.Context()
					defer cancel()

					opts := []grpc.DialOption{grpc.WithBlock(), grpc.WithInsecure()}
					if tc.multiplexed {
						nopServer := ServerFactory(func() Server { return grpc.NewServer() })
						opts = []grpc.DialOption{
							grpc.WithBlock(),
							grpc.WithTransportCredentials(nopServer.ClientHandshaker(
								testhelper.DiscardTestEntry(b), Insecure(),
							)),
						}
					}

					cc, err := grpc.DialContext(ctx, ln.Addr().String(), opts...)
					require.NoError(b, err)

					defer cc.Close()

					client, err := gitalypb.NewSSHServiceClient(cc).SSHUploadPack(ctx)
					require.NoError(b, err)

					request := &gitalypb.SSHUploadPackRequest{Stdin: make([]byte, messageSize)}
					b.SetBytes(messageSize)

					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						require.NoError(b, client.Send(request))
					}

					require.NoError(b, client.CloseSend())
					_, err = client.Recv()
					require.Equal(b, io.EOF, err)
				})
			}
		})
	}
}