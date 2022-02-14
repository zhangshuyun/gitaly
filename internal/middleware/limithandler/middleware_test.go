package limithandler_test

import (
	"bytes"
	"context"
	"net"
	"sync"
	"testing"
	"time"

	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/middleware/limithandler"
	pb "gitlab.com/gitlab-org/gitaly/v14/internal/middleware/limithandler/testdata"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"google.golang.org/grpc"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func fixedLockKey(ctx context.Context) string {
	return "fixed-id"
}

func TestUnaryLimitHandler(t *testing.T) {
	t.Parallel()

	s := &server{blockCh: make(chan struct{})}

	cfg := config.Cfg{
		Concurrency: []config.Concurrency{
			{RPC: "/test.limithandler.Test/Unary", MaxPerRepo: 2},
		},
	}

	lh := limithandler.New(cfg, fixedLockKey)
	interceptor := lh.UnaryInterceptor()
	srv, serverSocketPath := runServer(t, s, grpc.UnaryInterceptor(interceptor))
	defer srv.Stop()

	client, conn := newClient(t, serverSocketPath)
	defer conn.Close()
	ctx := testhelper.Context(t)

	wg := &sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			resp, err := client.Unary(ctx, &pb.UnaryRequest{})
			if !assert.NoError(t, err) {
				return
			}
			if !assert.NotNil(t, resp) {
				return
			}
			assert.True(t, resp.Ok)
		}()
	}

	time.Sleep(100 * time.Millisecond)

	require.Equal(t, 2, s.getRequestCount())

	close(s.blockCh)
	wg.Wait()
}

func TestStreamLimitHandler(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		desc                 string
		fullname             string
		f                    func(*testing.T, context.Context, pb.TestClient)
		maxConcurrency       int
		expectedRequestCount int
	}{
		{
			desc:     "Single request, multiple responses",
			fullname: "/test.limithandler.Test/StreamOutput",
			f: func(t *testing.T, ctx context.Context, client pb.TestClient) {
				stream, err := client.StreamOutput(ctx, &pb.StreamOutputRequest{})
				require.NoError(t, err)
				require.NotNil(t, stream)

				r, err := stream.Recv()
				require.NoError(t, err)
				require.NotNil(t, r)
				require.True(t, r.Ok)
			},
			maxConcurrency:       3,
			expectedRequestCount: 3,
		},
		{
			desc:     "Multiple requests, single response",
			fullname: "/test.limithandler.Test/StreamInput",
			f: func(t *testing.T, ctx context.Context, client pb.TestClient) {
				stream, err := client.StreamInput(ctx)
				require.NoError(t, err)
				require.NotNil(t, stream)

				require.NoError(t, stream.Send(&pb.StreamInputRequest{}))
				r, err := stream.CloseAndRecv()
				require.NoError(t, err)
				require.NotNil(t, r)
				require.True(t, r.Ok)
			},
			maxConcurrency:       3,
			expectedRequestCount: 3,
		},
		{
			desc:     "Multiple requests, multiple responses",
			fullname: "/test.limithandler.Test/Bidirectional",
			f: func(t *testing.T, ctx context.Context, client pb.TestClient) {
				stream, err := client.Bidirectional(ctx)
				require.NoError(t, err)
				require.NotNil(t, stream)

				require.NoError(t, stream.Send(&pb.BidirectionalRequest{}))
				require.NoError(t, stream.CloseSend())

				r, err := stream.Recv()
				require.NoError(t, err)
				require.NotNil(t, r)
				require.True(t, r.Ok)
			},
			maxConcurrency:       3,
			expectedRequestCount: 3,
		},
		{
			// Make sure that _streams_ are limited but that _requests_ on each
			// allowed stream are not limited.
			desc:     "Multiple requests with same id, multiple responses",
			fullname: "/test.limithandler.Test/Bidirectional",
			f: func(t *testing.T, ctx context.Context, client pb.TestClient) {
				stream, err := client.Bidirectional(ctx)
				require.NoError(t, err)
				require.NotNil(t, stream)

				// Since the concurrency id is fixed all requests have the same
				// id, but subsequent requests in a stream, even with the same
				// id, should bypass the concurrency limiter
				for i := 0; i < 10; i++ {
					require.NoError(t, stream.Send(&pb.BidirectionalRequest{}))
				}
				require.NoError(t, stream.CloseSend())

				r, err := stream.Recv()
				require.NoError(t, err)
				require.NotNil(t, r)
				require.True(t, r.Ok)
			},
			maxConcurrency: 3,
			// 3 (concurrent streams allowed) * 10 (requests per stream)
			expectedRequestCount: 30,
		},
		{
			desc:     "With a max concurrency of 0",
			fullname: "/test.limithandler.Test/StreamOutput",
			f: func(t *testing.T, ctx context.Context, client pb.TestClient) {
				stream, err := client.StreamOutput(ctx, &pb.StreamOutputRequest{})
				require.NoError(t, err)
				require.NotNil(t, stream)

				r, err := stream.Recv()
				require.NoError(t, err)
				require.NotNil(t, r)
				require.True(t, r.Ok)
			},
			maxConcurrency:       0,
			expectedRequestCount: 10, // Allow all
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			s := &server{blockCh: make(chan struct{})}

			cfg := config.Cfg{
				Concurrency: []config.Concurrency{
					{RPC: tc.fullname, MaxPerRepo: tc.maxConcurrency},
				},
			}

			lh := limithandler.New(cfg, fixedLockKey)
			interceptor := lh.StreamInterceptor()
			srv, serverSocketPath := runServer(t, s, grpc.StreamInterceptor(interceptor))
			defer srv.Stop()

			client, conn := newClient(t, serverSocketPath)
			defer conn.Close()
			ctx := testhelper.Context(t)

			wg := &sync.WaitGroup{}
			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					tc.f(t, ctx, client)
				}()
			}

			time.Sleep(100 * time.Millisecond)

			require.Equal(t, tc.expectedRequestCount, s.getRequestCount())

			close(s.blockCh)
			wg.Wait()
		})
	}
}

type queueTestServer struct {
	server
	reqArrivedCh chan struct{}
}

func (q *queueTestServer) Unary(ctx context.Context, in *pb.UnaryRequest) (*pb.UnaryResponse, error) {
	q.registerRequest()

	q.reqArrivedCh <- struct{}{} // We need a way to know when a request got to the middleware
	<-q.blockCh                  // Block to ensure concurrency

	return &pb.UnaryResponse{Ok: true}, nil
}

func TestLimitHandlerMetrics(t *testing.T) {
	s := &queueTestServer{reqArrivedCh: make(chan struct{})}
	s.blockCh = make(chan struct{})

	methodName := "/test.limithandler.Test/Unary"
	cfg := config.Cfg{
		Concurrency: []config.Concurrency{
			{RPC: methodName, MaxPerRepo: 1, MaxQueueSize: 1},
		},
	}

	lh := limithandler.New(cfg, fixedLockKey)
	interceptor := lh.UnaryInterceptor()
	srv, serverSocketPath := runServer(t, s, grpc.UnaryInterceptor(interceptor))
	defer srv.Stop()

	client, conn := newClient(t, serverSocketPath)
	defer conn.Close()

	ctx := featureflag.IncomingCtxWithFeatureFlag(
		testhelper.Context(t),
		featureflag.ConcurrencyQueueEnforceMax,
		true,
	)

	go func() {
		_, err := client.Unary(ctx, &pb.UnaryRequest{})
		require.NoError(t, err)
	}()
	// wait until the first request is being processed. After this, requests will be queued
	<-s.reqArrivedCh

	respCh := make(chan *pb.UnaryResponse)
	errChan := make(chan error)
	// out of ten requests, the first one will be queued and the other 9 will return with
	// an error
	for i := 0; i < 10; i++ {
		go func() {
			resp, err := client.Unary(ctx, &pb.UnaryRequest{})
			if err != nil {
				errChan <- err
			} else {
				respCh <- resp
			}
		}()
	}

	var errs int
	for err := range errChan {
		testhelper.RequireGrpcError(t, limithandler.ErrMaxQueueSize, err)
		errs++
		if errs == 9 {
			break
		}
	}

	expectedMetrics := `# HELP gitaly_rate_limiting_in_progress Gauge of number of concurrent in-progress calls
# TYPE gitaly_rate_limiting_in_progress gauge
gitaly_rate_limiting_in_progress{grpc_method="ReplicateRepository",grpc_service="gitaly.RepositoryService",system="gitaly"} 0
gitaly_rate_limiting_in_progress{grpc_method="Unary",grpc_service="test.limithandler.Test",system="gitaly"} 1
# HELP gitaly_rate_limiting_queued Gauge of number of queued calls
# TYPE gitaly_rate_limiting_queued gauge
gitaly_rate_limiting_queued{grpc_method="ReplicateRepository",grpc_service="gitaly.RepositoryService",system="gitaly"} 0
gitaly_rate_limiting_queued{grpc_method="Unary",grpc_service="test.limithandler.Test",system="gitaly"} 1
# HELP gitaly_requests_dropped_total Number of requests dropped from the queue
# TYPE gitaly_requests_dropped_total counter
gitaly_requests_dropped_total{grpc_method="Unary",grpc_service="test.limithandler.Test",reason="max_size",system="gitaly"} 9
`
	assert.NoError(t, promtest.CollectAndCompare(lh, bytes.NewBufferString(expectedMetrics),
		"gitaly_rate_limiting_queued",
		"gitaly_requests_dropped_total",
		"gitaly_rate_limiting_in_progress"))

	close(s.blockCh)
	<-s.reqArrivedCh
	<-respCh
}

func runServer(t *testing.T, s pb.TestServer, opt ...grpc.ServerOption) (*grpc.Server, string) {
	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName(t)
	grpcServer := grpc.NewServer(opt...)
	pb.RegisterTestServer(grpcServer, s)

	lis, err := net.Listen("unix", serverSocketPath)
	require.NoError(t, err)

	go grpcServer.Serve(lis)

	return grpcServer, "unix://" + serverSocketPath
}

func newClient(t *testing.T, serverSocketPath string) (pb.TestClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return pb.NewTestClient(conn), conn
}
