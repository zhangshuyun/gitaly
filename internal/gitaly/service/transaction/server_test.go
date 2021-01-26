package transaction

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

func TestServer_RouteVote(t *testing.T) {
	srvAddr, stopSrv := runServer(t)
	defer stopSrv()

	rtClient, cc := newClient(t, srvAddr)
	defer cc.Close()

	routeID1 := "1"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("when route ID is not opened", func(t *testing.T) {
		bidi, err := rtClient.RouteVote(ctx)
		require.NoError(t, err)
		defer func() { _ = bidi.CloseSend() }()

		errMsg := "rpc error: code = NotFound desc = route does not exist for UUID 1"

		t.Run("VoteTransaction fails", func(t *testing.T) {
			voteTxReq := &gitalypb.VoteTransactionRequest{RouteUuid: routeID1}
			_, err := rtClient.VoteTransaction(ctx, voteTxReq)
			require.Error(t, err)
			testhelper.RequireGrpcError(t, err, codes.NotFound)
			require.Contains(t, err.Error(), errMsg)
		})

		t.Run("StopTransaction fails", func(t *testing.T) {
			stopTxReq := &gitalypb.StopTransactionRequest{RouteUuid: routeID1}
			_, err := rtClient.StopTransaction(ctx, stopTxReq)
			require.Error(t, err)
			testhelper.RequireGrpcError(t, err, codes.NotFound)
			require.Contains(t, err.Error(), errMsg)
		})
	})

	// opens a session and waits for confirmation of setup
	openTxSession := func(t *testing.T, bidi gitalypb.RefTransaction_RouteVoteClient, routeID string) {
		expectResponse := &gitalypb.RouteVoteRequest{
			RouteUuid: routeID,
			Msg: &gitalypb.RouteVoteRequest_OpenRouteRequest{
				&gitalypb.RouteVoteRequest_OpenRoute{},
			},
		}
		err := bidi.Send(expectResponse)
		require.NoError(t, err)

		// wait for confirmation of message to avoid race condition
		actualResponse, err := bidi.Recv()
		require.NoError(t, err)
		testhelper.ProtoEqual(t, expectResponse, actualResponse)
	}

	// recvAndSend simulates a Praefect client receiving a request (recv)
	// from a Gitaly client, and then replying with a response (send)
	recvAndSend := func(t *testing.T, bidi gitalypb.RefTransaction_RouteVoteClient, recv proto.Message, send *gitalypb.RouteVoteRequest) {
		actualRecv, err := bidi.Recv()
		if !assert.NoError(t, err) {
			return
		}
		if !assert.True(t, proto.Equal(recv, actualRecv)) {
			return
		}

		err = bidi.Send(send)
		if !assert.NoError(t, err) {
			return
		}
	}

	// performs the Gitaly client RPC, and the Praefect handling of that
	// call and verifies all expected messages are routed correctly
	assertVoteRequestSucceeds := func(t *testing.T, ctx context.Context, routeID string, bidi gitalypb.RefTransaction_RouteVoteClient, request *gitalypb.VoteTransactionRequest, response *gitalypb.VoteTransactionResponse) {
		done := make(chan struct{})
		defer func() { <-done }()
		go func() {
			defer close(done)
			recv := &gitalypb.RouteVoteRequest{
				RouteUuid: routeID,
				Msg: &gitalypb.RouteVoteRequest_VoteTxRequest{
					request,
				},
			}
			send := &gitalypb.RouteVoteRequest{
				RouteUuid: routeID,
				Msg: &gitalypb.RouteVoteRequest_VoteTxResponse{
					response,
				},
			}
			recvAndSend(t, bidi, recv, send)
		}()

		actualResp, err := rtClient.VoteTransaction(ctx, request)
		require.NoError(t, err)
		testhelper.ProtoEqual(t, response, actualResp)
	}

	// performs a Gitaly RPC, and the Praefect handling of that call will
	// send back an error and verifies all messages are routed correctly
	assertVoteRequestFailure := func(t *testing.T, ctx context.Context, routeID string, bidi gitalypb.RefTransaction_RouteVoteClient, request *gitalypb.VoteTransactionRequest, statusErr *gitalypb.RouteVoteRequest_Status) {
		done := make(chan struct{})
		defer func() { <-done }()
		go func() {
			defer close(done)
			recv := &gitalypb.RouteVoteRequest{
				RouteUuid: routeID,
				Msg: &gitalypb.RouteVoteRequest_VoteTxRequest{
					request,
				},
			}
			send := &gitalypb.RouteVoteRequest{
				RouteUuid: routeID,
				Msg: &gitalypb.RouteVoteRequest_Error{
					statusErr,
				},
			}
			recvAndSend(t, bidi, recv, send)
		}()

		_, err := rtClient.VoteTransaction(ctx, request)
		require.Error(t, err)
		require.Equal(t, codes.Code(statusErr.Code), status.Code(err))
		require.Contains(t, err.Error(), statusErr.Message)
	}

	// performs the Gitaly client RPC, and the Praefect handling of that
	// call and verifies all expected messages are routed correctly
	assertStopRequestSucceeds := func(t *testing.T, ctx context.Context, routeID string, bidi gitalypb.RefTransaction_RouteVoteClient, request *gitalypb.StopTransactionRequest, response *gitalypb.StopTransactionResponse) {
		done := make(chan struct{})
		defer func() { <-done }()
		go func() {
			defer close(done)
			recv := &gitalypb.RouteVoteRequest{
				RouteUuid: routeID,
				Msg: &gitalypb.RouteVoteRequest_StopTxRequest{
					request,
				},
			}
			send := &gitalypb.RouteVoteRequest{
				RouteUuid: routeID,
				Msg: &gitalypb.RouteVoteRequest_StopTxResponse{
					response,
				},
			}
			recvAndSend(t, bidi, recv, send)
		}()

		actualResp, err := rtClient.StopTransaction(ctx, request)
		require.NoError(t, err)
		testhelper.ProtoEqual(t, response, actualResp)
	}

	// performs a Gitaly RPC, and the Praefect handling of that call will
	// send back an error and verifies all messages are routed correctly
	assertStopRequestFailure := func(t *testing.T, ctx context.Context, routeID string, bidi gitalypb.RefTransaction_RouteVoteClient, request *gitalypb.StopTransactionRequest, statusErr *gitalypb.RouteVoteRequest_Status) {
		done := make(chan struct{})
		defer func() { <-done }()
		go func() {
			defer close(done)
			recv := &gitalypb.RouteVoteRequest{
				RouteUuid: routeID,
				Msg: &gitalypb.RouteVoteRequest_StopTxRequest{
					request,
				},
			}
			send := &gitalypb.RouteVoteRequest{
				RouteUuid: routeID,
				Msg: &gitalypb.RouteVoteRequest_Error{
					statusErr,
				},
			}
			recvAndSend(t, bidi, recv, send)
		}()

		_, err := rtClient.StopTransaction(ctx, request)
		require.Error(t, err)
		require.Equal(t, codes.Code(statusErr.Code), status.Code(err))
		require.Contains(t, err.Error(), statusErr.Message)
	}

	t.Run("route opened", func(t *testing.T) {
		bidi, err := rtClient.RouteVote(ctx)
		require.NoError(t, err)
		defer func() { _ = bidi.CloseSend() }()

		openTxSession(t, bidi, routeID1)

		t.Run("opening additional route fails", func(t *testing.T) {
			bidi, err := rtClient.RouteVote(ctx)
			require.NoError(t, err)
			defer func() { _ = bidi.CloseSend() }()

			err = bidi.Send(&gitalypb.RouteVoteRequest{
				RouteUuid: routeID1,
				Msg: &gitalypb.RouteVoteRequest_OpenRouteRequest{
					&gitalypb.RouteVoteRequest_OpenRoute{},
				},
			})
			require.NoError(t, err)

			_, err = bidi.Recv()
			require.Error(t, err)

			testhelper.RequireGrpcError(t, err, codes.AlreadyExists)
		})

		t.Run("VoteTransaction request succeeds", func(t *testing.T) {
			request := &gitalypb.VoteTransactionRequest{
				RouteUuid: routeID1,
				Node:      "test",
			}
			response := &gitalypb.VoteTransactionResponse{
				State: gitalypb.VoteTransactionResponse_COMMIT,
			}
			assertVoteRequestSucceeds(t, ctx, routeID1, bidi, request, response)
		})

		t.Run("VoteTransaction request fails", func(t *testing.T) {
			request := &gitalypb.VoteTransactionRequest{
				RouteUuid: routeID1,
				Node:      "test",
			}
			statusErr := &gitalypb.RouteVoteRequest_Status{
				Code:    42,
				Message: "nope, try again",
			}
			assertVoteRequestFailure(t, ctx, routeID1, bidi, request, statusErr)
		})

		t.Run("StopTransaction request succeeds", func(t *testing.T) {
			request := &gitalypb.StopTransactionRequest{
				RouteUuid: routeID1,
			}
			response := &gitalypb.StopTransactionResponse{}
			assertStopRequestSucceeds(t, ctx, routeID1, bidi, request, response)
		})

		t.Run("StopTransaction request fails", func(t *testing.T) {
			request := &gitalypb.StopTransactionRequest{
				RouteUuid: routeID1,
			}
			statusErr := &gitalypb.RouteVoteRequest_Status{
				Code:    42,
				Message: "nope, try again",
			}
			assertStopRequestFailure(t, ctx, routeID1, bidi, request, statusErr)
		})
	})

	t.Run("multiple routes opened from many Praefects", func(t *testing.T) {
		bidiClients := make([]gitalypb.RefTransaction_RouteVoteClient, 1000)

		// open many concurrent route sessions
		for i := 0; i < 1000; i++ {
			bidi, err := rtClient.RouteVote(ctx)
			require.NoError(t, err)
			defer func() { _ = bidi.CloseSend() }()

			openTxSession(t, bidi, fmt.Sprint(i))

			bidiClients[i] = bidi
		}

		t.Run("VoteTransaction RPC routing", func(t *testing.T) {
			// send many requests
			for i := 0; i < 1000; i++ {
				request := &gitalypb.VoteTransactionRequest{
					RouteUuid: fmt.Sprint(i),
					Node:      fmt.Sprint(i),
				}
				response := &gitalypb.VoteTransactionResponse{
					State: gitalypb.VoteTransactionResponse_STOP,
				}

				assertVoteRequestSucceeds(t, ctx, fmt.Sprint(i), bidiClients[i], request, response)

				statusErr := &gitalypb.RouteVoteRequest_Status{
					Code:    42,
					Message: fmt.Sprint(i),
				}

				assertVoteRequestFailure(t, ctx, fmt.Sprint(i), bidiClients[i], request, statusErr)
			}
		})

		t.Run("StopTransaction RPC routing", func(t *testing.T) {
			// send many requests
			for i := 0; i < 1000; i++ {
				request := &gitalypb.StopTransactionRequest{
					RouteUuid: fmt.Sprint(i),
				}
				response := &gitalypb.StopTransactionResponse{}

				assertStopRequestSucceeds(t, ctx, fmt.Sprint(i), bidiClients[i], request, response)

				statusErr := &gitalypb.RouteVoteRequest_Status{
					Code:    42,
					Message: fmt.Sprint(i),
				}

				assertStopRequestFailure(t, ctx, fmt.Sprint(i), bidiClients[i], request, statusErr)
			}
		})
	})
}

func runServer(t *testing.T) (string, func()) {
	srv := testhelper.NewServer(t, nil, nil)

	gitalypb.RegisterRefTransactionServer(srv.GrpcServer(), NewServer())
	reflection.Register(srv.GrpcServer())

	srv.Start(t)

	return "unix://" + srv.Socket(), srv.Stop
}

func newClient(t *testing.T, serverSocketPath string) (gitalypb.RefTransactionClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}

	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewRefTransactionClient(conn), conn
}

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	cleanup := testhelper.Configure()
	defer cleanup()

	return m.Run()
}
