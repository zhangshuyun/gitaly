package streamrpc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	testpb "gitlab.com/gitlab-org/gitaly/v14/internal/streamrpc/testdata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestCall(t *testing.T) {
	const (
		testKey   = "test key"
		testValue = "test value"
		blobSize  = 1024 * 1024
	)

	var receivedValues []string
	var receivedField string

	dial := startServer(
		t,
		NewServer(),
		func(ctx context.Context, in *testpb.StreamRequest) (*emptypb.Empty, error) {
			receivedField = in.StringField

			if md, ok := metadata.FromIncomingContext(ctx); ok {
				receivedValues = md[testKey]
			}

			c, err := AcceptConnection(ctx)
			if err != nil {
				return nil, err
			}

			_, err = io.CopyN(c, c, blobSize)
			return nil, err
		},
	)

	in := make([]byte, blobSize)
	_, err := rand.Read(in)
	require.NoError(t, err)

	var out []byte
	require.NotEqual(t, in, out)

	ctx := metadata.AppendToOutgoingContext(context.Background(), testKey, testValue)
	require.NoError(t, Call(
		ctx,
		dial,
		"/test.streamrpc.Test/Stream",
		&testpb.StreamRequest{StringField: "hello world"},
		func(c net.Conn) error {
			errC := make(chan error, 1)
			go func() {
				var err error
				out, err = ioutil.ReadAll(c)
				errC <- err
			}()

			if _, err := io.Copy(c, bytes.NewReader(in)); err != nil {
				return err
			}
			if err := <-errC; err != nil {
				return err
			}

			return c.Close()
		},
	))

	require.Equal(t, "hello world", receivedField, "request propagates")
	require.Equal(t, []string{testValue}, receivedValues, "grpc metadata stored in client ctx propagates")
	require.Equal(t, in, out, "byte stream works")
}

func TestCall_serverError(t *testing.T) {
	dial := startServer(
		t,
		NewServer(),
		func(ctx context.Context, in *testpb.StreamRequest) (*emptypb.Empty, error) {
			return nil, errors.New("this is the server error")
		},
	)

	callError := Call(
		context.Background(),
		dial,
		"/test.streamrpc.Test/Stream",
		&testpb.StreamRequest{},
		func(c net.Conn) error { panic("never reached") },
	)

	require.Equal(t, &RequestRejectedError{"this is the server error"}, callError)
}

func TestCall_clientMiddleware(t *testing.T) {
	const (
		testKey   = "test key"
		testValue = "test value"
	)

	var receivedValues []string
	var receivedField string

	dial := startServer(
		t,
		NewServer(),
		func(ctx context.Context, in *testpb.StreamRequest) (*emptypb.Empty, error) {
			_, err := AcceptConnection(ctx)
			return nil, err
		},
	)

	var middlewareMethod string
	ctx := metadata.AppendToOutgoingContext(context.Background(), testKey, testValue)

	const testMethod = "/test.streamrpc.Test/Stream"
	require.NoError(t, Call(
		ctx,
		dial,
		testMethod,
		&testpb.StreamRequest{StringField: "hello world"},
		func(c net.Conn) error { return nil },
		WithClientInterceptor(func(ctx context.Context, method string, req, _ interface{}, _ *grpc.ClientConn, invoker grpc.UnaryInvoker, _ ...grpc.CallOption) error {
			middlewareMethod = method
			receivedField = req.(*testpb.StreamRequest).StringField
			if md, ok := metadata.FromOutgoingContext(ctx); ok {
				receivedValues = md[testKey]
			}
			return invoker(ctx, method, req, nil, nil)
		}),
	))

	require.Equal(t, testMethod, middlewareMethod, "client middleware sees correct method")
	require.Equal(t, "hello world", receivedField, "client middleware sees request")
	require.Equal(t, []string{testValue}, receivedValues, "client middleware sees context metadata")
}

func TestCall_clientMiddlewareReject(t *testing.T) {
	dial := startServer(
		t,
		NewServer(),
		func(ctx context.Context, in *testpb.StreamRequest) (*emptypb.Empty, error) {
			panic("never reached")
		},
	)

	middlewareError := errors.New("middleware says no")

	err := Call(
		context.Background(),
		dial,
		"/test.streamrpc.Test/Stream",
		&testpb.StreamRequest{StringField: "hello world"},
		func(c net.Conn) error { return nil },
		WithClientInterceptor(func(ctx context.Context, method string, req, _ interface{}, _ *grpc.ClientConn, invoker grpc.UnaryInvoker, _ ...grpc.CallOption) error {
			return middlewareError
		}),
	)

	require.Equal(t, middlewareError, err)
}

func TestCall_serverMiddleware(t *testing.T) {
	const (
		testKey    = "test key"
		testValue  = "test value"
		testMethod = "/test.streamrpc.Test/Stream"
	)

	var (
		receivedField    string
		middlewareMethod string
		receivedValues   []string
	)

	interceptorDone := make(chan struct{})

	dial := startServer(
		t,
		NewServer(WithServerInterceptor(func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
			defer close(interceptorDone)
			middlewareMethod = info.FullMethod
			receivedField = req.(*testpb.StreamRequest).StringField
			if md, ok := metadata.FromIncomingContext(ctx); ok {
				receivedValues = md[testKey]
			}
			return handler(ctx, req)
		})),
		func(ctx context.Context, in *testpb.StreamRequest) (*emptypb.Empty, error) {
			_, err := AcceptConnection(ctx)
			return nil, err
		},
	)

	ctx := metadata.AppendToOutgoingContext(context.Background(), testKey, testValue)
	require.NoError(t, Call(
		ctx,
		dial,
		testMethod,
		&testpb.StreamRequest{StringField: "hello world"},
		func(c net.Conn) error { return nil },
	))

	<-interceptorDone
	require.Equal(t, testMethod, middlewareMethod, "server middleware sees correct method")
	require.Equal(t, "hello world", receivedField, "server middleware sees request")
	require.Equal(t, []string{testValue}, receivedValues, "server middleware sees context metadata")
}

func TestCall_serverMiddlewareReject(t *testing.T) {
	dial := startServer(
		t,
		NewServer(WithServerInterceptor(func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
			return nil, errors.New("middleware says no")
		})),
		func(ctx context.Context, in *testpb.StreamRequest) (*emptypb.Empty, error) {
			panic("never reached")
		},
	)

	err := Call(
		context.Background(),
		dial,
		"/test.streamrpc.Test/Stream",
		&testpb.StreamRequest{},
		func(c net.Conn) error { return nil },
	)

	require.Equal(t, &RequestRejectedError{message: "middleware says no"}, err)
}

type testCredentials struct {
	values map[string]string
}

func (tc *testCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	out := make(map[string]string)
	for k, v := range tc.values {
		out[k] = v
	}
	return out, nil
}

func (*testCredentials) RequireTransportSecurity() bool { return false }

func TestCall_credentials(t *testing.T) {
	receivedValues := make(map[string]string)
	interceptorDone := make(chan struct{})

	dial := startServer(
		t,
		NewServer(),
		func(ctx context.Context, in *testpb.StreamRequest) (*emptypb.Empty, error) {
			defer close(interceptorDone)

			if md, ok := metadata.FromIncomingContext(ctx); ok {
				receivedValues["key 1"] = strings.Join(md["key 1"], ",")
				receivedValues["key 2"] = strings.Join(md["key 2"], ",")
			}

			_, err := AcceptConnection(ctx)
			return nil, err
		},
	)

	inputs := map[string]string{
		"key 1": "value a",
		"key 2": "value b",
	}

	require.NoError(t, Call(
		context.Background(),
		dial,
		"/test.streamrpc.Test/Stream",
		&testpb.StreamRequest{},
		func(c net.Conn) error { return nil },
		WithCredentials(&testCredentials{inputs}),
	))

	<-interceptorDone
	require.Equal(t, inputs, receivedValues)
}

func startServer(t *testing.T, s *Server, th testHandler) DialFunc {
	t.Helper()
	testpb.RegisterTestServer(s, &server{testHandler: th})
	client, server := socketPair(t)
	go func() { _ = s.Handle(server) }()
	return func(time.Duration) (net.Conn, error) { return client, nil }
}

type testHandler func(ctx context.Context, in *testpb.StreamRequest) (*emptypb.Empty, error)

type server struct {
	testpb.UnimplementedTestServer
	testHandler
}

func (s *server) Stream(ctx context.Context, in *testpb.StreamRequest) (*emptypb.Empty, error) {
	return s.testHandler(ctx, in)
}
