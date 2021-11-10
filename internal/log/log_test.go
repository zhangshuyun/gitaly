package log

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
)

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
		require.Equal(t, context.Background(), ctx)
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
	})(context.Background(), "format-stub", logrus.DebugLevel, codes.OutOfRange, assert.AnError, logrus.Fields{"c": "stub"})
	require.True(t, triggered)
}

func TestPropagationMessageProducer(t *testing.T) {
	t.Run("empty context", func(t *testing.T) {
		ctx := context.Background()
		mp := PropagationMessageProducer(func(context.Context, string, logrus.Level, codes.Code, error, logrus.Fields) {})
		mp(ctx, "", logrus.DebugLevel, codes.OK, nil, nil)
	})

	t.Run("context with holder", func(t *testing.T) {
		holder := new(messageProducerHolder)
		ctx := context.WithValue(context.Background(), messageProducerHolderKey{}, holder)
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
		ctx := context.Background()
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
		ctx := ctxlogrus.ToContext(context.Background(), logrus.NewEntry(logrus.New()))
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
