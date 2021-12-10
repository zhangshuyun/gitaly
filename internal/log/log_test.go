package log

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	grpcmw "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcmwlogrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/grpcstats"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/test/grpc_testing"
	"google.golang.org/protobuf/proto"
)

func TestPayloadBytes(t *testing.T) {
	ctx := createContext()

	logger, hook := test.NewNullLogger()

	opts := []grpc.ServerOption{
		grpc.StatsHandler(PerRPCLogHandler{
			Underlying:     &grpcstats.PayloadBytes{},
			FieldProducers: []FieldsProducer{grpcstats.FieldsProducer},
		}),
		grpc.UnaryInterceptor(
			grpcmw.ChainUnaryServer(
				grpcmwlogrus.UnaryServerInterceptor(
					logrus.NewEntry(logger),
					grpcmwlogrus.WithMessageProducer(
						MessageProducer(
							PropagationMessageProducer(grpcmwlogrus.DefaultMessageProducer),
							grpcstats.FieldsProducer,
						),
					),
				),
				UnaryLogDataCatcherServerInterceptor(),
			),
		),
		grpc.StreamInterceptor(
			grpcmw.ChainStreamServer(
				grpcmwlogrus.StreamServerInterceptor(
					logrus.NewEntry(logger),
					grpcmwlogrus.WithMessageProducer(
						MessageProducer(
							PropagationMessageProducer(grpcmwlogrus.DefaultMessageProducer),
							grpcstats.FieldsProducer,
						),
					),
				),
				StreamLogDataCatcherServerInterceptor(),
			),
		),
	}

	srv := grpc.NewServer(opts...)
	grpc_testing.RegisterTestServiceServer(srv, testService{})
	sock, err := os.CreateTemp("", "")
	require.NoError(t, err)
	require.NoError(t, sock.Close())
	require.NoError(t, os.RemoveAll(sock.Name()))
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(sock.Name())) })

	lis, err := net.Listen("unix", sock.Name())
	require.NoError(t, err)

	t.Cleanup(srv.GracefulStop)
	go func() { assert.NoError(t, srv.Serve(lis)) }()

	cc, err := client.DialContext(ctx, "unix://"+sock.Name(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cc.Close()) })

	testClient := grpc_testing.NewTestServiceClient(cc)
	const invocations = 2
	var wg sync.WaitGroup
	for i := 0; i < invocations; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			resp, err := testClient.UnaryCall(ctx, &grpc_testing.SimpleRequest{Payload: newStubPayload()})
			if !assert.NoError(t, err) {
				return
			}
			require.Equal(t, newStubPayload(), resp.Payload)

			call, err := testClient.HalfDuplexCall(ctx)
			if !assert.NoError(t, err) {
				return
			}

			done := make(chan struct{})
			go func() {
				defer close(done)
				for {
					_, err := call.Recv()
					if err == io.EOF {
						return
					}
					assert.NoError(t, err)
				}
			}()
			assert.NoError(t, call.Send(&grpc_testing.StreamingOutputCallRequest{Payload: newStubPayload()}))
			assert.NoError(t, call.Send(&grpc_testing.StreamingOutputCallRequest{Payload: newStubPayload()}))
			assert.NoError(t, call.CloseSend())
			<-done
		}()
	}
	wg.Wait()

	srv.GracefulStop()

	entries := hook.AllEntries()
	require.Len(t, entries, 4)
	var unary, stream int
	for _, e := range entries {
		if e.Message == "finished unary call with code OK" {
			unary++
			require.EqualValues(t, 8, e.Data["grpc.request.payload_bytes"])
			require.EqualValues(t, 8, e.Data["grpc.response.payload_bytes"])
		}
		if e.Message == "finished streaming call with code OK" {
			stream++
			require.EqualValues(t, 16, e.Data["grpc.request.payload_bytes"])
			require.EqualValues(t, 16, e.Data["grpc.response.payload_bytes"])
		}
	}
	require.Equal(t, invocations, unary)
	require.Equal(t, invocations, stream)
}

func newStubPayload() *grpc_testing.Payload {
	return &grpc_testing.Payload{Body: []byte("stub")}
}

type testService struct {
	grpc_testing.UnimplementedTestServiceServer
}

func (ts testService) UnaryCall(context.Context, *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error) {
	return &grpc_testing.SimpleResponse{Payload: newStubPayload()}, nil
}

func (ts testService) HalfDuplexCall(stream grpc_testing.TestService_HalfDuplexCallServer) error {
	for {
		if _, err := stream.Recv(); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
	}

	resp := &grpc_testing.StreamingOutputCallResponse{Payload: newStubPayload()}
	if err := stream.Send(proto.Clone(resp).(*grpc_testing.StreamingOutputCallResponse)); err != nil {
		return err
	}
	return stream.Send(proto.Clone(resp).(*grpc_testing.StreamingOutputCallResponse))
}

func TestConfigure(t *testing.T) {
	for _, tc := range []struct {
		desc   string
		format string
		level  string
		logger *logrus.Logger
	}{
		{
			desc:   "json format with info level",
			format: "json",
			logger: &logrus.Logger{
				Formatter: &utcFormatter{&logrus.JSONFormatter{TimestampFormat: LogTimestampFormatUTC}},
				Level:     logrus.InfoLevel,
			},
		},
		{
			desc:   "text format with info level",
			format: "text",
			logger: &logrus.Logger{
				Formatter: &utcFormatter{&logrus.TextFormatter{TimestampFormat: LogTimestampFormatUTC}},
				Level:     logrus.InfoLevel,
			},
		},
		{
			desc: "empty format with info level",
			logger: &logrus.Logger{
				Formatter: &utcFormatter{&logrus.TextFormatter{TimestampFormat: LogTimestampFormatUTC}},
				Level:     logrus.InfoLevel,
			},
		},
		{
			desc:   "text format with debug level",
			format: "text",
			level:  "debug",
			logger: &logrus.Logger{
				Formatter: &utcFormatter{&logrus.TextFormatter{TimestampFormat: LogTimestampFormatUTC}},
				Level:     logrus.DebugLevel,
			},
		},
		{
			desc:   "text format with invalid level",
			format: "text",
			level:  "invalid-level",
			logger: &logrus.Logger{
				Formatter: &utcFormatter{&logrus.TextFormatter{TimestampFormat: LogTimestampFormatUTC}},
				Level:     logrus.InfoLevel,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			loggers := []*logrus.Logger{{Formatter: &logrus.TextFormatter{}}, {Formatter: &logrus.TextFormatter{}}}
			Configure(loggers, tc.format, tc.level)
			require.Equal(t, []*logrus.Logger{tc.logger, tc.logger}, loggers)

			now := time.Now()
			nowUTCFormatted := now.UTC().Format(LogTimestampFormatUTC)

			message := "this is a logging message."
			var out bytes.Buffer

			// both loggers are the same, so no need to test both the same way
			logger := loggers[0]
			logger.Out = &out
			entry := logger.WithTime(now)

			switch tc.level {
			case "debug":
				entry.Debug(message)
			case "warn":
				entry.Warn(message)
			case "error":
				entry.Error(message)
			case "", "info":
				entry.Info(message)
			default:
				entry.Info(message)
			}

			if tc.format != "" {
				assert.Contains(t, out.String(), nowUTCFormatted)
			}
			assert.Contains(t, out.String(), message)
		})
	}
}

func TestMessageProducer(t *testing.T) {
	triggered := false
	MessageProducer(func(ctx context.Context, format string, level logrus.Level, code codes.Code, err error, fields logrus.Fields) {
		require.Equal(t, createContext(), ctx)
		require.Equal(t, "format-stub", format)
		require.Equal(t, logrus.DebugLevel, level)
		require.Equal(t, codes.OutOfRange, code)
		require.Equal(t, assert.AnError, err)
		require.Equal(t, logrus.Fields{"a": 1, "b": "test", "c": "stub"}, fields)
		triggered = true
	}, func(context.Context) logrus.Fields {
		return logrus.Fields{"a": 1}
	}, func(context.Context) logrus.Fields {
		return logrus.Fields{"b": "test"}
	})(createContext(), "format-stub", logrus.DebugLevel, codes.OutOfRange, assert.AnError, logrus.Fields{"c": "stub"})
	require.True(t, triggered)
}

func TestPropagationMessageProducer(t *testing.T) {
	t.Run("empty context", func(t *testing.T) {
		ctx := createContext()
		mp := PropagationMessageProducer(func(context.Context, string, logrus.Level, codes.Code, error, logrus.Fields) {})
		mp(ctx, "", logrus.DebugLevel, codes.OK, nil, nil)
	})

	t.Run("context with holder", func(t *testing.T) {
		holder := new(messageProducerHolder)
		ctx := context.WithValue(createContext(), messageProducerHolderKey{}, holder)
		triggered := false
		mp := PropagationMessageProducer(func(ctx context.Context, format string, level logrus.Level, code codes.Code, err error, fields logrus.Fields) {
			triggered = true
		})
		mp(ctx, "format-stub", logrus.DebugLevel, codes.OutOfRange, assert.AnError, logrus.Fields{"a": 1})
		require.Equal(t, "format-stub", holder.format)
		require.Equal(t, logrus.DebugLevel, holder.level)
		require.Equal(t, codes.OutOfRange, holder.code)
		require.Equal(t, assert.AnError, holder.err)
		require.Equal(t, logrus.Fields{"a": 1}, holder.fields)
		holder.actual(ctx, "", logrus.DebugLevel, codes.OK, nil, nil)
		require.True(t, triggered)
	})
}

func TestPerRPCLogHandler(t *testing.T) {
	msh := &mockStatHandler{Calls: map[string][]interface{}{}}

	lh := PerRPCLogHandler{
		Underlying: msh,
		FieldProducers: []FieldsProducer{
			func(ctx context.Context) logrus.Fields { return logrus.Fields{"a": 1} },
			func(ctx context.Context) logrus.Fields { return logrus.Fields{"b": "2"} },
		},
	}

	t.Run("check propagation", func(t *testing.T) {
		ctx := createContext()
		ctx = lh.TagConn(ctx, &stats.ConnTagInfo{})
		lh.HandleConn(ctx, &stats.ConnBegin{})
		ctx = lh.TagRPC(ctx, &stats.RPCTagInfo{})
		lh.HandleRPC(ctx, &stats.Begin{})
		lh.HandleRPC(ctx, &stats.InHeader{})
		lh.HandleRPC(ctx, &stats.InPayload{})
		lh.HandleRPC(ctx, &stats.OutHeader{})
		lh.HandleRPC(ctx, &stats.OutPayload{})
		lh.HandleRPC(ctx, &stats.End{})
		lh.HandleConn(ctx, &stats.ConnEnd{})

		assert.Equal(t, map[string][]interface{}{
			"TagConn":    {&stats.ConnTagInfo{}},
			"HandleConn": {&stats.ConnBegin{}, &stats.ConnEnd{}},
			"TagRPC":     {&stats.RPCTagInfo{}},
			"HandleRPC":  {&stats.Begin{}, &stats.InHeader{}, &stats.InPayload{}, &stats.OutHeader{}, &stats.OutPayload{}, &stats.End{}},
		}, msh.Calls)
	})

	t.Run("log handling", func(t *testing.T) {
		ctx := ctxlogrus.ToContext(createContext(), logrus.NewEntry(logrus.New()))
		ctx = lh.TagRPC(ctx, &stats.RPCTagInfo{})
		mpp := ctx.Value(messageProducerHolderKey{}).(*messageProducerHolder)
		mpp.format = "message"
		mpp.level = logrus.InfoLevel
		mpp.code = codes.InvalidArgument
		mpp.err = assert.AnError
		mpp.actual = func(ctx context.Context, format string, level logrus.Level, code codes.Code, err error, fields logrus.Fields) {
			assert.Equal(t, "message", format)
			assert.Equal(t, logrus.InfoLevel, level)
			assert.Equal(t, codes.InvalidArgument, code)
			assert.Equal(t, assert.AnError, err)
			assert.Equal(t, logrus.Fields{"a": 1, "b": "2"}, mpp.fields)
		}
		lh.HandleRPC(ctx, &stats.End{})
	})
}

type mockStatHandler struct {
	Calls map[string][]interface{}
}

func (m *mockStatHandler) TagRPC(ctx context.Context, s *stats.RPCTagInfo) context.Context {
	m.Calls["TagRPC"] = append(m.Calls["TagRPC"], s)
	return ctx
}

func (m *mockStatHandler) HandleRPC(ctx context.Context, s stats.RPCStats) {
	m.Calls["HandleRPC"] = append(m.Calls["HandleRPC"], s)
}

func (m *mockStatHandler) TagConn(ctx context.Context, s *stats.ConnTagInfo) context.Context {
	m.Calls["TagConn"] = append(m.Calls["TagConn"], s)
	return ctx
}

func (m *mockStatHandler) HandleConn(ctx context.Context, s stats.ConnStats) {
	m.Calls["HandleConn"] = append(m.Calls["HandleConn"], s)
}

func TestUnaryLogDataCatcherServerInterceptor(t *testing.T) {
	handlerStub := func(context.Context, interface{}) (interface{}, error) {
		return nil, nil
	}

	t.Run("propagates call", func(t *testing.T) {
		interceptor := UnaryLogDataCatcherServerInterceptor()
		resp, err := interceptor(createContext(), nil, nil, func(ctx context.Context, req interface{}) (interface{}, error) {
			return 42, assert.AnError
		})

		assert.Equal(t, 42, resp)
		assert.Equal(t, assert.AnError, err)
	})

	t.Run("no logger", func(t *testing.T) {
		mpp := &messageProducerHolder{}
		ctx := context.WithValue(createContext(), messageProducerHolderKey{}, mpp)

		interceptor := UnaryLogDataCatcherServerInterceptor()
		_, _ = interceptor(ctx, nil, nil, handlerStub)
		assert.Empty(t, mpp.fields)
	})

	t.Run("caught", func(t *testing.T) {
		mpp := &messageProducerHolder{}
		ctx := context.WithValue(createContext(), messageProducerHolderKey{}, mpp)
		ctx = ctxlogrus.ToContext(ctx, logrus.New().WithField("a", 1))
		interceptor := UnaryLogDataCatcherServerInterceptor()
		_, _ = interceptor(ctx, nil, nil, handlerStub)
		assert.Equal(t, logrus.Fields{"a": 1}, mpp.fields)
	})
}

func TestStreamLogDataCatcherServerInterceptor(t *testing.T) {
	t.Run("propagates call", func(t *testing.T) {
		interceptor := StreamLogDataCatcherServerInterceptor()
		ss := &grpcmw.WrappedServerStream{WrappedContext: createContext()}
		err := interceptor(nil, ss, nil, func(interface{}, grpc.ServerStream) error {
			return assert.AnError
		})

		assert.Equal(t, assert.AnError, err)
	})

	t.Run("no logger", func(t *testing.T) {
		mpp := &messageProducerHolder{}
		ctx := context.WithValue(createContext(), messageProducerHolderKey{}, mpp)

		interceptor := StreamLogDataCatcherServerInterceptor()
		ss := &grpcmw.WrappedServerStream{WrappedContext: ctx}
		_ = interceptor(nil, ss, nil, func(interface{}, grpc.ServerStream) error { return nil })
	})

	t.Run("caught", func(t *testing.T) {
		mpp := &messageProducerHolder{}
		ctx := context.WithValue(createContext(), messageProducerHolderKey{}, mpp)
		ctx = ctxlogrus.ToContext(ctx, logrus.New().WithField("a", 1))

		interceptor := StreamLogDataCatcherServerInterceptor()
		ss := &grpcmw.WrappedServerStream{WrappedContext: ctx}
		_ = interceptor(nil, ss, nil, func(interface{}, grpc.ServerStream) error { return nil })
		assert.Equal(t, logrus.Fields{"a": 1}, mpp.fields)
	})
}

//nolint:forbidigo // We cannot use `testhelper.Context()` because of a cyclic dependency between
// this package and the `testhelper` package.
func createContext() context.Context {
	return context.Background()
}
