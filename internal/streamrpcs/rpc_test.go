package streamrpcs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v14/auth"
	"gitlab.com/gitlab-org/gitaly/v14/internal/bootstrap/starter"
	"gitlab.com/gitlab-org/gitaly/v14/internal/listenmux"
	gitalylog "gitlab.com/gitlab-org/gitaly/v14/internal/log"
	testpb "gitlab.com/gitlab-org/gitaly/v14/internal/streamrpcs/testdata"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestCall(t *testing.T) {
	const blobSize = 1024 * 1024

	var receivedField string

	in := make([]byte, blobSize)
	_, err := rand.Read(in)
	require.NoError(t, err)

	var out []byte
	require.NotEqual(t, in, out)

	client, addr := startServer(
		t,
		func(stream testpb.Test_StreamServer) error {
			request, err := stream.Recv()
			require.NoError(t, err)

			receivedField = request.StringField
			conn, err := AcceptConnection(stream.Context(), stream)
			if err != nil {
				return err
			}

			if _, err = io.CopyN(conn, conn, blobSize); err != nil {
				return err
			}

			return conn.Close()
		},
	)

	ctx := context.Background()
	require.NoError(t, Call(
		ctx, addr, handshake(client), DialNet(),
		func(conn net.Conn) error {
			errC := make(chan error, 1)
			go func() {
				var err error
				out, err = ioutil.ReadAll(conn)
				errC <- err
			}()

			_, err = io.Copy(conn, bytes.NewReader(in))
			require.NoError(t, err)
			require.NoError(t, <-errC)

			return nil
		},
	))

	require.Equal(t, "hello world", receivedField, "request propagates")
	require.Equal(t, in, out, "byte stream works")
}

func TestCall_serverError(t *testing.T) {
	client, addr := startServer(
		t,
		func(stream testpb.Test_StreamServer) error {
			_, err := stream.Recv()
			if err != nil {
				return err
			}

			conn, err := AcceptConnection(stream.Context(), stream)
			if err != nil {
				return err
			}
			defer conn.Close()

			return errors.New("this is the server error")
		},
	)
	ctx := context.Background()
	require.EqualError(t, Call(
		ctx, addr, handshake(client), DialNet(),
		func(conn net.Conn) error { return nil },
	), "rpc error: code = Unknown desc = this is the server error")
}

func TestCall_serverMiddleware(t *testing.T) {
	const (
		testKey    = "testkey"
		testValue  = "testvalue"
		testMethod = "/test.streamrpc.Test/Stream"
	)

	var (
		middlewareMethod string
		receivedValues   []string
	)

	interceptorDone := make(chan struct{})

	client, addr := startServer(
		t,
		func(stream testpb.Test_StreamServer) error {
			_, err := stream.Recv()
			if err != nil {
				return err
			}

			conn, err := AcceptConnection(stream.Context(), stream)
			if err != nil {
				return err
			}
			defer conn.Close()

			return nil
		},
		grpc.StreamInterceptor(func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			defer close(interceptorDone)
			middlewareMethod = info.FullMethod
			if md, ok := metadata.FromIncomingContext(ss.Context()); ok {
				receivedValues = md[testKey]
			}
			return handler(srv, ss)
		}),
	)

	ctx := metadata.AppendToOutgoingContext(context.Background(), testKey, testValue)
	require.NoError(t, Call(
		ctx, addr, handshake(client), DialNet(),
		func(conn net.Conn) error { return nil },
	))

	<-interceptorDone
	require.Equal(t, testMethod, middlewareMethod, "server middleware sees correct method")
	require.Equal(t, []string{testValue}, receivedValues, "server middleware sees context metadata")
}

func TestCall_serverMiddlewareReject(t *testing.T) {
	client, addr := startServer(
		t,
		func(stream testpb.Test_StreamServer) error {
			panic("never reached")
		},
		grpc.StreamInterceptor(func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			return status.Errorf(codes.PermissionDenied, "permission denied")
		}),
	)

	ctx := context.Background()
	testhelper.RequireGrpcError(t, Call(
		ctx, addr, handshake(client), DialNet(),
		func(conn net.Conn) error { return nil },
	), codes.PermissionDenied)
}

func TestCall_credentials(t *testing.T) {
	var receivedValue string
	interceptorDone := make(chan struct{})

	_, addr := startServer(
		t,
		func(stream testpb.Test_StreamServer) error {
			defer close(interceptorDone)
			ctx := stream.Context()

			if md, ok := metadata.FromIncomingContext(ctx); ok {
				receivedValue = md.Get("authorization")[0]
			}

			_, err := stream.Recv()
			if err != nil {
				return err
			}

			conn, err := AcceptConnection(ctx, stream)
			if err != nil {
				return err
			}
			defer conn.Close()

			return nil
		},
	)

	endpoint, _ := starter.ParseEndpoint(addr)
	conn, err := grpc.Dial(
		endpoint.Addr,
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2("foobar")),
		grpc.WithInsecure(),
	)
	require.NoError(t, err)

	client := testpb.NewTestClient(conn)

	ctx := context.Background()
	require.NoError(t, Call(
		ctx, addr, handshake(client), DialNet(),
		func(conn net.Conn) error { return nil },
	), codes.PermissionDenied)

	<-interceptorDone
	require.Contains(t, receivedValue, "Bearer v2.")
}

func TestCall_clientRetries(t *testing.T) {
	t.Run("error before receiving the first request", func(t *testing.T) {
		failure := 2
		client, addr := startServer(
			t,
			func(stream testpb.Test_StreamServer) error {
				if failure > 0 {
					failure--
					return errors.New("server rejected")
				}
				_, err := stream.Recv()
				if err != nil {
					return err
				}

				conn, err := AcceptConnection(stream.Context(), stream)
				if err != nil {
					return err
				}
				defer conn.Close()

				return nil
			},
		)
		ctx := context.Background()
		require.NoError(t, Call(
			ctx, addr, handshake(client), DialNet(),
			func(conn net.Conn) error { return nil },
		))
		require.Zero(t, failure)
	})

	t.Run("error before waiting for the connection", func(t *testing.T) {
		failure := 2
		client, addr := startServer(
			t,
			func(stream testpb.Test_StreamServer) error {
				_, err := stream.Recv()
				if err != nil {
					return err
				}

				if failure > 0 {
					failure--
					return errors.New("server closed unexpected")
				}

				conn, err := AcceptConnection(stream.Context(), stream)
				if err != nil {
					return err
				}
				defer conn.Close()

				return nil
			},
		)
		ctx := context.Background()
		require.NoError(t, Call(
			ctx, addr, handshake(client), DialNet(),
			func(conn net.Conn) error { return nil },
		))
		require.Zero(t, failure)
	})

	t.Run("error after connection establishment", func(t *testing.T) {
		failure := 2
		client, addr := startServer(
			t,
			func(stream testpb.Test_StreamServer) error {
				_, err := stream.Recv()
				if err != nil {
					return err
				}

				conn, err := AcceptConnection(stream.Context(), stream)
				if err != nil {
					return err
				}
				defer conn.Close()

				if failure > 0 {
					failure--
					return errors.New("server closed unexpected")
				}
				return nil
			},
		)
		ctx := context.Background()
		require.NoError(t, Call(
			ctx, addr, handshake(client), DialNet(),
			func(conn net.Conn) error { return nil },
		))
		require.Zero(t, failure)
	})
}

func startServer(t *testing.T, th testHandler, opts ...grpc.ServerOption) (testpb.TestClient, string) {
	t.Helper()

	transportCredentials := insecure.NewCredentials()
	lm := listenmux.New(transportCredentials)
	lm.Register(NewServerHandshaker(
		gitalylog.Default(),
	))
	opts = append(opts, grpc.Creds(lm))

	s := grpc.NewServer(opts...)
	t.Cleanup(func() { s.Stop() })

	handler := &server{testHandler: th}
	testpb.RegisterTestServer(s, handler)

	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	t.Cleanup(func() { lis.Close() })

	go func() { s.Serve(lis) }()

	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithInsecure())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	client := testpb.NewTestClient(conn)

	return client, "tcp://" + lis.Addr().String()
}

func handshake(client testpb.TestClient) func(context.Context) (grpc.ClientStream, error) {
	return func(ctx context.Context) (grpc.ClientStream, error) {
		stream, err := client.Stream(ctx)
		if err != nil {
			return stream, err
		}
		if err = stream.Send(&testpb.StreamRequest{StringField: "hello world"}); err != nil {
			return stream, err
		}
		return stream, nil
	}
}

type testHandler func(stream testpb.Test_StreamServer) error

type server struct {
	testpb.UnimplementedTestServer
	testHandler
}

func (s *server) Stream(stream testpb.Test_StreamServer) error {
	return s.testHandler(stream)
}
