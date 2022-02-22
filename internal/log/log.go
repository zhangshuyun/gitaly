package log

import (
	"context"
	"os"
	"regexp"

	grpcmwlogging "github.com/grpc-ecosystem/go-grpc-middleware/logging"
	grpcmwlogrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/env"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
)

const (
	// GitalyLogDirEnvKey defines the environment variable used to specify the Gitaly log directory
	GitalyLogDirEnvKey = "GITALY_LOG_DIR"
	// LogTimestampFormat defines the timestamp format in log files
	LogTimestampFormat = "2006-01-02T15:04:05.000"
	// LogTimestampFormatUTC defines the utc timestamp format in log files
	LogTimestampFormatUTC = "2006-01-02T15:04:05.000Z"

	defaultLogRequestMethodAllowPattern = ""
	defaultLogRequestMethodDenyPattern  = "^/grpc.health.v1.Health/Check$"
)

type utcFormatter struct {
	logrus.Formatter
}

func (u utcFormatter) Format(e *logrus.Entry) ([]byte, error) {
	e.Time = e.Time.UTC()
	return u.Formatter.Format(e)
}

// UTCJsonFormatter returns a Formatter that formats a logrus Entry's as json and converts the time
// field into UTC
func UTCJsonFormatter() logrus.Formatter {
	return &utcFormatter{Formatter: &logrus.JSONFormatter{TimestampFormat: LogTimestampFormatUTC}}
}

// UTCTextFormatter returns a Formatter that formats a logrus Entry's as text and converts the time
// field into UTC
func UTCTextFormatter() logrus.Formatter {
	return &utcFormatter{Formatter: &logrus.TextFormatter{TimestampFormat: LogTimestampFormatUTC}}
}

var (
	defaultLogger = logrus.StandardLogger()
	grpcGo        = logrus.New()

	// Loggers is convenient when you want to apply configuration to all
	// loggers
	Loggers = []*logrus.Logger{defaultLogger, grpcGo}
)

func init() {
	// This ensures that any log statements that occur before
	// the configuration has been loaded will be written to
	// stdout instead of stderr
	for _, l := range Loggers {
		l.Out = os.Stdout
	}
}

// Configure sets the format and level on all loggers. It applies level
// mapping to the GrpcGo logger.
func Configure(loggers []*logrus.Logger, format string, level string) {
	var formatter logrus.Formatter
	switch format {
	case "json":
		formatter = UTCJsonFormatter()
	case "", "text":
		formatter = UTCTextFormatter()
	default:
		logrus.WithField("format", format).Fatal("invalid logger format")
	}

	logrusLevel, err := logrus.ParseLevel(level)
	if err != nil {
		logrusLevel = logrus.InfoLevel
	}

	for _, l := range loggers {
		if l == grpcGo {
			l.SetLevel(mapGrpcLogLevel(logrusLevel))
		} else {
			l.SetLevel(logrusLevel)
		}

		if formatter != nil {
			l.Formatter = formatter
		}
	}
}

// DeciderOption returns a Option to support log filtering.
// If "GITALY_LOG_REQUEST_METHOD_DENY_PATTERN" ENV variable is set, logger will filter out the log whose "fullMethodName" matches it;
// If "GITALY_LOG_REQUEST_METHOD_ALLOW_PATTERN" ENV variable is set, logger will only keep the log whose "fullMethodName" matches it;
// Under any conditions, the error log will not be filtered out;
// If the ENV variables are not set, there will be no additional effects.
func DeciderOption() grpcmwlogrus.Option {
	matcher := methodNameMatcherFromEnv()

	if matcher == nil {
		return grpcmwlogrus.WithDecider(grpcmwlogging.DefaultDeciderMethod)
	}

	decider := func(fullMethodName string, err error) bool {
		if err != nil {
			return true
		}
		return matcher(fullMethodName)
	}

	return grpcmwlogrus.WithDecider(decider)
}

func methodNameMatcherFromEnv() func(string) bool {
	if pattern := env.GetString("GITALY_LOG_REQUEST_METHOD_ALLOW_PATTERN",
		defaultLogRequestMethodAllowPattern); pattern != "" {
		methodRegex := regexp.MustCompile(pattern)

		return func(fullMethodName string) bool {
			return methodRegex.MatchString(fullMethodName)
		}
	}

	if pattern := env.GetString("GITALY_LOG_REQUEST_METHOD_DENY_PATTERN",
		defaultLogRequestMethodDenyPattern); pattern != "" {
		methodRegex := regexp.MustCompile(pattern)

		return func(fullMethodName string) bool {
			return !methodRegex.MatchString(fullMethodName)
		}
	}

	return nil
}

func mapGrpcLogLevel(level logrus.Level) logrus.Level {
	// Honor grpc-go's debug settings: https://github.com/grpc/grpc-go#how-to-turn-on-logging
	logLevel := os.Getenv("GRPC_GO_LOG_SEVERITY_LEVEL")
	if logLevel != "" {
		switch logLevel {
		case "ERROR", "error":
			return logrus.ErrorLevel
		case "WARNING", "warning":
			return logrus.WarnLevel
		case "INFO", "info":
			return logrus.InfoLevel
		}
	}

	// grpc-go is too verbose at level 'info'. So when config.toml requests
	// level info, we tell grpc-go to log at 'warn' instead.
	if level == logrus.InfoLevel {
		return logrus.WarnLevel
	}

	return level
}

// Default is the default logrus logger
func Default() *logrus.Entry { return defaultLogger.WithField("pid", os.Getpid()) }

// GrpcGo is a dedicated logrus logger for the grpc-go library. We use it
// to control the library's chattiness.
func GrpcGo() *logrus.Entry { return grpcGo.WithField("pid", os.Getpid()) }

// FieldsProducer returns fields that need to be added into the logging context.
type FieldsProducer func(context.Context) logrus.Fields

// MessageProducer returns a wrapper that extends passed mp to accept additional fields generated
// by each of the fieldsProducers.
func MessageProducer(mp grpcmwlogrus.MessageProducer, fieldsProducers ...FieldsProducer) grpcmwlogrus.MessageProducer {
	return func(ctx context.Context, format string, level logrus.Level, code codes.Code, err error, fields logrus.Fields) {
		for _, fieldsProducer := range fieldsProducers {
			for key, val := range fieldsProducer(ctx) {
				fields[key] = val
			}
		}
		mp(ctx, format, level, code, err, fields)
	}
}

type messageProducerHolder struct {
	logger *logrus.Entry
	actual grpcmwlogrus.MessageProducer
	format string
	level  logrus.Level
	code   codes.Code
	err    error
	fields logrus.Fields
}

type messageProducerHolderKey struct{}

// messageProducerPropagationFrom extracts *messageProducerHolder from context
// and returns to the caller.
// It returns nil in case it is not found.
func messageProducerPropagationFrom(ctx context.Context) *messageProducerHolder {
	mpp, ok := ctx.Value(messageProducerHolderKey{}).(*messageProducerHolder)
	if !ok {
		return nil
	}
	return mpp
}

// PropagationMessageProducer catches logging information from the context and populates it
// to the special holder that should be present in the context.
// Should be used only in combination with PerRPCLogHandler.
func PropagationMessageProducer(actual grpcmwlogrus.MessageProducer) grpcmwlogrus.MessageProducer {
	return func(ctx context.Context, format string, level logrus.Level, code codes.Code, err error, fields logrus.Fields) {
		mpp := messageProducerPropagationFrom(ctx)
		if mpp == nil {
			return
		}
		*mpp = messageProducerHolder{
			logger: ctxlogrus.Extract(ctx),
			actual: actual,
			format: format,
			level:  level,
			code:   code,
			err:    err,
			fields: fields,
		}
	}
}

// PerRPCLogHandler is designed to collect stats that are accessible
// from the google.golang.org/grpc/stats.Handler, because some information
// can't be extracted on the interceptors level.
type PerRPCLogHandler struct {
	Underlying     stats.Handler
	FieldProducers []FieldsProducer
}

// HandleConn only calls Underlying and exists to satisfy gRPC stats.Handler.
func (lh PerRPCLogHandler) HandleConn(ctx context.Context, cs stats.ConnStats) {
	lh.Underlying.HandleConn(ctx, cs)
}

// TagConn only calls Underlying and exists to satisfy gRPC stats.Handler.
func (lh PerRPCLogHandler) TagConn(ctx context.Context, cti *stats.ConnTagInfo) context.Context {
	return lh.Underlying.TagConn(ctx, cti)
}

// HandleRPC catches each RPC call and for the *stats.End stats invokes
// custom message producers to populate logging data. Once all data is collected
// the actual logging happens by using logger that is caught by PropagationMessageProducer.
func (lh PerRPCLogHandler) HandleRPC(ctx context.Context, rs stats.RPCStats) {
	lh.Underlying.HandleRPC(ctx, rs)
	switch rs.(type) {
	case *stats.End:
		// This code runs once all interceptors are finished their execution.
		// That is why any logging info collected after interceptors completion
		// is not at the logger's context. That is why we need to manually propagate
		// it to the logger.
		mpp := messageProducerPropagationFrom(ctx)
		if mpp == nil || (mpp != nil && mpp.actual == nil) {
			return
		}

		if mpp.fields == nil {
			mpp.fields = logrus.Fields{}
		}
		for _, fp := range lh.FieldProducers {
			for k, v := range fp(ctx) {
				mpp.fields[k] = v
			}
		}
		// Once again because all interceptors are finished and context doesn't contain
		// a logger we need to set logger manually into the context.
		// It's needed because github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus.DefaultMessageProducer
		// extracts logger from the context and use it to write the logs.
		ctx = ctxlogrus.ToContext(ctx, mpp.logger)
		mpp.actual(ctx, mpp.format, mpp.level, mpp.code, mpp.err, mpp.fields)
		return
	}
}

// TagRPC propagates a special data holder into the context that is responsible to
// hold logging information produced by the logging interceptor.
// The logging data should be caught by the UnaryLogDataCatcherServerInterceptor. It needs to
// be included into the interceptor chain below logging interceptor.
func (lh PerRPCLogHandler) TagRPC(ctx context.Context, rti *stats.RPCTagInfo) context.Context {
	ctx = context.WithValue(ctx, messageProducerHolderKey{}, new(messageProducerHolder))
	return lh.Underlying.TagRPC(ctx, rti)
}

// UnaryLogDataCatcherServerInterceptor catches logging data produced by the upper interceptors and
// propagates it into the holder to pop up it to the HandleRPC method of the PerRPCLogHandler.
func UnaryLogDataCatcherServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		mpp := messageProducerPropagationFrom(ctx)
		if mpp != nil {
			mpp.fields = ctxlogrus.Extract(ctx).Data
		}
		return handler(ctx, req)
	}
}

// StreamLogDataCatcherServerInterceptor catches logging data produced by the upper interceptors and
// propagates it into the holder to pop up it to the HandleRPC method of the PerRPCLogHandler.
func StreamLogDataCatcherServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()
		mpp := messageProducerPropagationFrom(ctx)
		if mpp != nil {
			mpp.fields = ctxlogrus.Extract(ctx).Data
		}
		return handler(srv, ss)
	}
}
