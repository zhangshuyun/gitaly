// Copyright 2017 Michal Witkowski. All Rights Reserved.
// See LICENSE for licensing terms.

package proxy_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/fieldextractors"
	"gitlab.com/gitlab-org/gitaly/v14/internal/middleware/sentryhandler"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/grpc-proxy/proxy"
	pb "gitlab.com/gitlab-org/gitaly/v14/internal/praefect/grpc-proxy/testdata"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testassert"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	pingDefaultValue   = "I like kittens."
	clientMdKey        = "test-client-header"
	serverHeaderMdKey  = "test-client-header"
	serverTrailerMdKey = "test-client-trailer"

	rejectingMdKey = "test-reject-rpc-if-in-context"

	countListResponses = 20
)

func TestMain(m *testing.M) {
	defer testhelper.MustHaveNoChildProcess()
	cleanup := testhelper.Configure()
	defer cleanup()

	goleak.VerifyTestMain(m)
}

// asserting service is implemented on the server side and serves as a handler for stuff
type assertingService struct {
	pb.UnimplementedTestServiceServer
	t *testing.T
}

func (s *assertingService) PingEmpty(ctx context.Context, _ *pb.Empty) (*pb.PingResponse, error) {
	// Check that this call has client's metadata.
	md, ok := metadata.FromIncomingContext(ctx)
	assert.True(s.t, ok, "PingEmpty call must have metadata in context")
	_, ok = md[clientMdKey]
	assert.True(s.t, ok, "PingEmpty call must have clients's custom headers in metadata")
	return &pb.PingResponse{Value: pingDefaultValue, Counter: 42}, nil
}

func (s *assertingService) Ping(ctx context.Context, ping *pb.PingRequest) (*pb.PingResponse, error) {
	// Send user trailers and headers.
	require.NoError(s.t, grpc.SendHeader(ctx, metadata.Pairs(serverHeaderMdKey, "I like turtles.")))
	require.NoError(s.t, grpc.SetTrailer(ctx, metadata.Pairs(serverTrailerMdKey, "I like ending turtles.")))
	return &pb.PingResponse{Value: ping.Value, Counter: 42}, nil
}

func (s *assertingService) PingError(ctx context.Context, ping *pb.PingRequest) (*pb.Empty, error) {
	return nil, status.Errorf(codes.ResourceExhausted, "Userspace error.")
}

func (s *assertingService) PingList(ping *pb.PingRequest, stream pb.TestService_PingListServer) error {
	// Send user trailers and headers.
	require.NoError(s.t, stream.SendHeader(metadata.Pairs(serverHeaderMdKey, "I like turtles.")))
	for i := 0; i < countListResponses; i++ {
		require.NoError(s.t, stream.Send(&pb.PingResponse{Value: ping.Value, Counter: int32(i)}))
	}
	stream.SetTrailer(metadata.Pairs(serverTrailerMdKey, "I like ending turtles."))
	return nil
}

func (s *assertingService) PingStream(stream pb.TestService_PingStreamServer) error {
	require.NoError(s.t, stream.SendHeader(metadata.Pairs(serverHeaderMdKey, "I like turtles.")))
	counter := int32(0)
	for {
		ping, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			require.NoError(s.t, err, "can't fail reading stream")
			return err
		}
		pong := &pb.PingResponse{Value: ping.Value, Counter: counter}
		if err := stream.Send(pong); err != nil {
			require.NoError(s.t, err, "can't fail sending back a pong")
		}
		counter++
	}
	stream.SetTrailer(metadata.Pairs(serverTrailerMdKey, "I like ending turtles."))
	return nil
}

// ProxyHappySuite tests the "happy" path of handling: that everything works in absence of connection issues.
type ProxyHappySuite struct {
	suite.Suite

	serverListener   net.Listener
	server           *grpc.Server
	proxyListener    net.Listener
	proxy            *grpc.Server
	serverClientConn *grpc.ClientConn

	client         *grpc.ClientConn
	testClient     pb.TestServiceClient
	testClientConn *grpc.ClientConn
}

func (s *ProxyHappySuite) ctx() context.Context {
	// Make all RPC calls last at most 1 sec, meaning all async issues or deadlock will not kill tests.
	ctx, _ := context.WithTimeout(context.TODO(), 120*time.Second) // nolint: govet
	return ctx
}

func (s *ProxyHappySuite) TestPingEmptyCarriesClientMetadata() {
	ctx := metadata.NewOutgoingContext(s.ctx(), metadata.Pairs(clientMdKey, "true"))
	out, err := s.testClient.PingEmpty(ctx, &pb.Empty{})
	require.NoError(s.T(), err, "PingEmpty should succeed without errors")
	testassert.ProtoEqual(s.T(), &pb.PingResponse{Value: pingDefaultValue, Counter: 42}, out)
}

func (s *ProxyHappySuite) TestPingEmpty_StressTest() {
	for i := 0; i < 50; i++ {
		s.TestPingEmptyCarriesClientMetadata()
	}
}

func (s *ProxyHappySuite) TestPingCarriesServerHeadersAndTrailers() {
	headerMd := make(metadata.MD)
	trailerMd := make(metadata.MD)
	// This is an awkward calling convention... but meh.
	out, err := s.testClient.Ping(s.ctx(), &pb.PingRequest{Value: "foo"}, grpc.Header(&headerMd), grpc.Trailer(&trailerMd))
	require.NoError(s.T(), err, "Ping should succeed without errors")
	testassert.ProtoEqual(s.T(), &pb.PingResponse{Value: "foo", Counter: 42}, out)
	assert.Contains(s.T(), headerMd, serverHeaderMdKey, "server response headers must contain server data")
	assert.Len(s.T(), trailerMd, 1, "server response trailers must contain server data")
}

func (s *ProxyHappySuite) TestPingErrorPropagatesAppError() {
	sentryTriggered := 0
	sentrySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sentryTriggered++
	}))
	defer sentrySrv.Close()

	// minimal required sentry client configuration
	sentryURL, err := url.Parse(sentrySrv.URL)
	require.NoError(s.T(), err)
	sentryURL.User = url.UserPassword("stub", "stub")
	sentryURL.Path = "/stub/1"

	require.NoError(s.T(), sentry.Init(sentry.ClientOptions{
		Dsn:       sentryURL.String(),
		Transport: sentry.NewHTTPSyncTransport(),
	}))

	sentry.CaptureEvent(sentry.NewEvent())
	require.Equal(s.T(), 1, sentryTriggered, "sentry configured incorrectly")

	_, err = s.testClient.PingError(s.ctx(), &pb.PingRequest{Value: "foo"})
	require.Error(s.T(), err, "PingError should never succeed")
	assert.Equal(s.T(), codes.ResourceExhausted, status.Code(err))
	assert.Equal(s.T(), "Userspace error.", status.Convert(err).Message())
	require.Equal(s.T(), 1, sentryTriggered, "sentry must not be triggered because errors from remote must be just propagated")
}

func (s *ProxyHappySuite) TestDirectorErrorIsPropagated() {
	// See SetupSuite where the StreamDirector has a special case.
	ctx := metadata.NewOutgoingContext(s.ctx(), metadata.Pairs(rejectingMdKey, "true"))
	_, err := s.testClient.Ping(ctx, &pb.PingRequest{Value: "foo"})
	require.Error(s.T(), err, "Director should reject this RPC")
	assert.Equal(s.T(), codes.PermissionDenied, status.Code(err))
	assert.Equal(s.T(), "testing rejection", status.Convert(err).Message())
}

func (s *ProxyHappySuite) TestPingStream_FullDuplexWorks() {
	stream, err := s.testClient.PingStream(s.ctx())
	require.NoError(s.T(), err, "PingStream request should be successful.")

	for i := 0; i < countListResponses; i++ {
		ping := &pb.PingRequest{Value: fmt.Sprintf("foo:%d", i)}
		require.NoError(s.T(), stream.Send(ping), "sending to PingStream must not fail")
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if i == 0 {
			// Check that the header arrives before all entries.
			headerMd, err := stream.Header()
			require.NoError(s.T(), err, "PingStream headers should not error.")
			assert.Contains(s.T(), headerMd, serverHeaderMdKey, "PingStream response headers user contain metadata")
		}
		assert.EqualValues(s.T(), i, resp.Counter, "ping roundtrip must succeed with the correct id")
	}
	require.NoError(s.T(), stream.CloseSend(), "no error on close send")
	_, err = stream.Recv()
	require.Equal(s.T(), io.EOF, err, "stream should close with io.EOF, meaining OK")
	// Check that the trailer headers are here.
	trailerMd := stream.Trailer()
	assert.Len(s.T(), trailerMd, 1, "PingList trailer headers user contain metadata")
}

func (s *ProxyHappySuite) TestPingStream_StressTest() {
	for i := 0; i < 50; i++ {
		s.TestPingStream_FullDuplexWorks()
	}
}

func (s *ProxyHappySuite) SetupSuite() {
	var err error

	s.proxyListener, err = net.Listen("tcp", "127.0.0.1:0")
	require.NoError(s.T(), err, "must be able to allocate a port for proxyListener")
	s.serverListener, err = net.Listen("tcp", "127.0.0.1:0")
	require.NoError(s.T(), err, "must be able to allocate a port for serverListener")

	s.server = grpc.NewServer()
	pb.RegisterTestServiceServer(s.server, &assertingService{t: s.T()})

	// Setup of the proxy's Director.
	s.serverClientConn, err = grpc.Dial(s.serverListener.Addr().String(), grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.ForceCodec(proxy.NewCodec())))
	require.NoError(s.T(), err, "must not error on deferred client Dial")
	director := func(ctx context.Context, fullName string, peeker proxy.StreamPeeker) (*proxy.StreamParameters, error) {
		payload, err := peeker.Peek()
		if err != nil {
			return nil, err
		}

		md, ok := metadata.FromIncomingContext(ctx)
		if ok {
			if _, exists := md[rejectingMdKey]; exists {
				return proxy.NewStreamParameters(proxy.Destination{Ctx: helper.IncomingToOutgoing(ctx), Msg: payload}, nil, nil, nil), status.Errorf(codes.PermissionDenied, "testing rejection")
			}
		}

		// Explicitly copy the metadata, otherwise the tests will fail.
		return proxy.NewStreamParameters(proxy.Destination{Ctx: helper.IncomingToOutgoing(ctx), Conn: s.serverClientConn, Msg: payload}, nil, nil, nil), nil
	}

	s.proxy = grpc.NewServer(
		grpc.ForceServerCodec(proxy.NewCodec()),
		grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(
				// context tags usage is required by sentryhandler.StreamLogHandler
				grpc_ctxtags.StreamServerInterceptor(grpc_ctxtags.WithFieldExtractorForInitialReq(fieldextractors.FieldExtractor)),
				// sentry middleware to capture errors
				sentryhandler.StreamLogHandler,
			),
		),
		grpc.UnknownServiceHandler(proxy.TransparentHandler(director)),
	)
	// Ping handler is handled as an explicit registration and not as a TransparentHandler.
	proxy.RegisterService(s.proxy, director,
		"mwitkow.testproto.TestService",
		"Ping")

	// Start the serving loops.
	go func() {
		s.server.Serve(s.serverListener)
	}()
	go func() {
		s.proxy.Serve(s.proxyListener)
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), 1*time.Second)
	defer cancel()

	s.testClientConn, err = grpc.DialContext(ctx, strings.Replace(s.proxyListener.Addr().String(), "127.0.0.1", "localhost", 1), grpc.WithInsecure())
	require.NoError(s.T(), err, "must not error on deferred client Dial")
	s.testClient = pb.NewTestServiceClient(s.testClientConn)
}

func (s *ProxyHappySuite) TearDownSuite() {
	if s.client != nil {
		s.client.Close()
	}
	if s.testClientConn != nil {
		s.testClientConn.Close()
	}
	if s.serverClientConn != nil {
		s.serverClientConn.Close()
	}
	// Close all transports so the logs don't get spammy.
	time.Sleep(10 * time.Millisecond)
	if s.proxy != nil {
		s.proxy.Stop()
		s.proxyListener.Close()
	}
	if s.serverListener != nil {
		s.server.Stop()
		s.serverListener.Close()
	}
}

func TestProxyHappySuite(t *testing.T) {
	suite.Run(t, &ProxyHappySuite{})
}

func TestProxyErrorPropagation(t *testing.T) {
	errBackend := status.Error(codes.InvalidArgument, "backend error")
	errDirector := status.Error(codes.FailedPrecondition, "director error")
	errRequestFinalizer := status.Error(codes.Internal, "request finalizer error")

	for _, tc := range []struct {
		desc                  string
		backendError          error
		directorError         error
		requestFinalizerError error
		returnedError         error
		errHandler            func(error) error
	}{
		{
			desc:          "backend error is propagated",
			backendError:  errBackend,
			returnedError: errBackend,
		},
		{
			desc:          "director error is propagated",
			directorError: errDirector,
			returnedError: errDirector,
		},
		{
			desc:                  "request finalizer error is propagated",
			requestFinalizerError: errRequestFinalizer,
			returnedError:         errRequestFinalizer,
		},
		{
			desc:                  "director error cancels proxying",
			backendError:          errBackend,
			requestFinalizerError: errRequestFinalizer,
			directorError:         errDirector,
			returnedError:         errDirector,
		},
		{
			desc:                  "backend error prioritized over request finalizer error",
			backendError:          errBackend,
			requestFinalizerError: errRequestFinalizer,
			returnedError:         errBackend,
		},
		{
			desc:                  "err handler gets error",
			backendError:          errBackend,
			requestFinalizerError: errRequestFinalizer,
			returnedError:         errBackend,
			errHandler: func(err error) error {
				testassert.GrpcEqualErr(t, errBackend, err)
				return errBackend
			},
		},
		{
			desc:          "err handler can swallow error",
			backendError:  errBackend,
			returnedError: io.EOF,
			errHandler: func(err error) error {
				testassert.GrpcEqualErr(t, errBackend, err)
				return nil
			},
		},
		{
			desc:                  "swallowed error surfaces request finalizer error",
			backendError:          errBackend,
			requestFinalizerError: errRequestFinalizer,
			returnedError:         errRequestFinalizer,
			errHandler: func(err error) error {
				testassert.GrpcEqualErr(t, errBackend, err)
				return nil
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			tmpDir := testhelper.TempDir(t)

			backendListener, err := net.Listen("unix", filepath.Join(tmpDir, "backend"))
			require.NoError(t, err)

			backendServer := grpc.NewServer(grpc.UnknownServiceHandler(func(interface{}, grpc.ServerStream) error {
				return tc.backendError
			}))
			go func() { backendServer.Serve(backendListener) }()
			defer backendServer.Stop()

			ctx, cancel := testhelper.Context()
			defer cancel()

			backendClientConn, err := grpc.DialContext(ctx, "unix://"+backendListener.Addr().String(),
				grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.ForceCodec(proxy.NewCodec())))
			require.NoError(t, err)
			defer func() {
				require.NoError(t, backendClientConn.Close())
			}()

			proxyListener, err := net.Listen("unix", filepath.Join(tmpDir, "proxy"))
			require.NoError(t, err)

			proxyServer := grpc.NewServer(
				grpc.ForceServerCodec(proxy.NewCodec()),
				grpc.UnknownServiceHandler(proxy.TransparentHandler(func(ctx context.Context, fullMethodName string, peeker proxy.StreamPeeker) (*proxy.StreamParameters, error) {
					return proxy.NewStreamParameters(
						proxy.Destination{
							Ctx:        ctx,
							Conn:       backendClientConn,
							ErrHandler: tc.errHandler,
						},
						nil,
						func() error { return tc.requestFinalizerError },
						nil,
					), tc.directorError
				})),
			)

			go func() { proxyServer.Serve(proxyListener) }()
			defer proxyServer.Stop()

			proxyClientConn, err := grpc.DialContext(ctx, "unix://"+proxyListener.Addr().String(), grpc.WithInsecure())
			require.NoError(t, err)
			defer func() {
				require.NoError(t, proxyClientConn.Close())
			}()

			resp, err := pb.NewTestServiceClient(proxyClientConn).Ping(ctx, &pb.PingRequest{})
			testassert.GrpcEqualErr(t, tc.returnedError, err)
			require.Nil(t, resp)
		})
	}
}

func TestRegisterStreamHandlers(t *testing.T) {
	directorCalledError := errors.New("director was called")

	server := grpc.NewServer(
		grpc.ForceServerCodec(proxy.NewCodec()),
		grpc.UnknownServiceHandler(proxy.TransparentHandler(func(ctx context.Context, fullMethodName string, peeker proxy.StreamPeeker) (*proxy.StreamParameters, error) {
			return nil, directorCalledError
		})),
	)

	var pingStreamHandlerCalled, pingEmptyStreamHandlerCalled bool

	pingValue := "hello"

	pingStreamHandler := func(srv interface{}, stream grpc.ServerStream) error {
		pingStreamHandlerCalled = true
		var req pb.PingRequest

		if err := stream.RecvMsg(&req); err != nil {
			return err
		}

		require.Equal(t, pingValue, req.Value)

		return stream.SendMsg(nil)
	}

	pingEmptyStreamHandler := func(srv interface{}, stream grpc.ServerStream) error {
		pingEmptyStreamHandlerCalled = true
		var req pb.Empty

		if err := stream.RecvMsg(&req); err != nil {
			return err
		}

		return stream.SendMsg(nil)
	}

	streamers := map[string]grpc.StreamHandler{
		"Ping":      pingStreamHandler,
		"PingEmpty": pingEmptyStreamHandler,
	}

	proxy.RegisterStreamHandlers(server, "mwitkow.testproto.TestService", streamers)

	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName(t)

	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	go server.Serve(listener)
	defer server.Stop()

	cc, err := client.Dial("unix://"+serverSocketPath, []grpc.DialOption{grpc.WithBlock()})
	require.NoError(t, err)
	defer cc.Close()

	testServiceClient := pb.NewTestServiceClient(cc)

	ctx, cancel := testhelper.Context()
	defer cancel()

	_, err = testServiceClient.Ping(ctx, &pb.PingRequest{Value: pingValue})
	require.NoError(t, err)
	require.True(t, pingStreamHandlerCalled)

	_, err = testServiceClient.PingEmpty(ctx, &pb.Empty{})
	require.NoError(t, err)
	require.True(t, pingEmptyStreamHandlerCalled)

	// since PingError was never registered with its own streamer, it should get sent to the UnknownServiceHandler
	_, err = testServiceClient.PingError(ctx, &pb.PingRequest{})
	testassert.GrpcEqualErr(t, status.Error(codes.Unknown, directorCalledError.Error()), err)
}
