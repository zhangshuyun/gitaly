package limithandler_test

import (
	"context"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/middleware/limithandler"
	pb "gitlab.com/gitlab-org/gitaly/v14/internal/middleware/limithandler/testdata"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"google.golang.org/grpc"
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()
	cleanup := testhelper.Configure()
	defer cleanup()
	return m.Run()
}

func fixedLockKey(ctx context.Context) string {
	return "fixed-id"
}

func TestUnaryLimitHandler(t *testing.T) {
	s := &server{blockCh: make(chan struct{})}

	limithandler.SetMaxRepoConcurrency(map[string]int{"/test.Test/Unary": 2})
	lh := limithandler.New(fixedLockKey)
	interceptor := lh.UnaryInterceptor()
	srv, serverSocketPath := runServer(t, s, grpc.UnaryInterceptor(interceptor))
	defer srv.Stop()

	client, conn := newClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	wg := &sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			resp, err := client.Unary(ctx, &pb.UnaryRequest{})
			require.NotNil(t, resp)
			require.NoError(t, err)
			require.True(t, resp.Ok)
		}()
	}

	time.Sleep(100 * time.Millisecond)

	require.Equal(t, 2, s.getRequestCount())

	close(s.blockCh)
	wg.Wait()
}

func TestStreamLimitHandler(t *testing.T) {
	testCases := []struct {
		desc                 string
		fullname             string
		f                    func(*testing.T, context.Context, pb.TestClient)
		maxConcurrency       int
		expectedRequestCount int
	}{
		{
			desc:     "Single request, multiple responses",
			fullname: "/test.Test/StreamOutput",
			f: func(t *testing.T, ctx context.Context, client pb.TestClient) {
				stream, err := client.StreamOutput(ctx, &pb.StreamOutputRequest{})
				require.NotNil(t, stream)
				require.NoError(t, err)

				r, err := stream.Recv()
				require.NotNil(t, r)
				require.NoError(t, err)
				require.True(t, r.Ok)
			},
			maxConcurrency:       3,
			expectedRequestCount: 3,
		},
		{
			desc:     "Multiple requests, single response",
			fullname: "/test.Test/StreamInput",
			f: func(t *testing.T, ctx context.Context, client pb.TestClient) {
				stream, err := client.StreamInput(ctx)
				require.NotNil(t, stream)
				require.NoError(t, err)

				require.NoError(t, stream.Send(&pb.StreamInputRequest{}))
				r, err := stream.CloseAndRecv()
				require.NotNil(t, r)
				require.NoError(t, err)
				require.True(t, r.Ok)
			},
			maxConcurrency:       3,
			expectedRequestCount: 3,
		},
		{
			desc:     "Multiple requests, multiple responses",
			fullname: "/test.Test/Bidirectional",
			f: func(t *testing.T, ctx context.Context, client pb.TestClient) {
				stream, err := client.Bidirectional(ctx)
				require.NotNil(t, stream)
				require.NoError(t, err)

				require.NoError(t, stream.Send(&pb.BidirectionalRequest{}))
				require.NoError(t, stream.CloseSend())

				r, err := stream.Recv()
				require.NotNil(t, r)
				require.NoError(t, err)
				require.True(t, r.Ok)
			},
			maxConcurrency:       3,
			expectedRequestCount: 3,
		},
		{
			// Make sure that _streams_ are limited but that _requests_ on each
			// allowed stream are not limited.
			desc:     "Multiple requests with same id, multiple responses",
			fullname: "/test.Test/Bidirectional",
			f: func(t *testing.T, ctx context.Context, client pb.TestClient) {
				stream, err := client.Bidirectional(ctx)
				require.NotNil(t, stream)
				require.NoError(t, err)

				// Since the concurrency id is fixed all requests have the same
				// id, but subsequent requests in a stream, even with the same
				// id, should bypass the concurrency limiter
				for i := 0; i < 10; i++ {
					require.NoError(t, stream.Send(&pb.BidirectionalRequest{}))
				}
				require.NoError(t, stream.CloseSend())

				r, err := stream.Recv()
				require.NotNil(t, r)
				require.NoError(t, err)
				require.True(t, r.Ok)
			},
			maxConcurrency: 3,
			// 3 (concurrent streams allowed) * 10 (requests per stream)
			expectedRequestCount: 30,
		},
		{
			desc:     "With a max concurrency of 0",
			fullname: "/test.Test/StreamOutput",
			f: func(t *testing.T, ctx context.Context, client pb.TestClient) {
				stream, err := client.StreamOutput(ctx, &pb.StreamOutputRequest{})
				require.NotNil(t, stream)
				require.NoError(t, err)

				r, err := stream.Recv()
				require.NotNil(t, r)
				require.NoError(t, err)
				require.True(t, r.Ok)
			},
			maxConcurrency:       0,
			expectedRequestCount: 10, // Allow all
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			s := &server{blockCh: make(chan struct{})}

			limithandler.SetMaxRepoConcurrency(map[string]int{
				tc.fullname: tc.maxConcurrency,
			})

			lh := limithandler.New(fixedLockKey)
			interceptor := lh.StreamInterceptor()
			srv, serverSocketPath := runServer(t, s, grpc.StreamInterceptor(interceptor))
			defer srv.Stop()

			client, conn := newClient(t, serverSocketPath)
			defer conn.Close()

			ctx, cancel := testhelper.Context()
			defer cancel()

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

func runServer(t *testing.T, s *server, opt ...grpc.ServerOption) (*grpc.Server, string) {
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
