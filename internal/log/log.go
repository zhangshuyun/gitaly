package log

import (
	"os"

	"github.com/sirupsen/logrus"
)

const (
	// GitalyLogDirEnvKey defines the environment variable used to specify the Gitaly log directory
	GitalyLogDirEnvKey = "GITALY_LOG_DIR"
	// LogTimestampFormat defines the timestamp format in log files
	LogTimestampFormat = "2006-01-02T15:04:05.000"
	// LogTimestampFormatUTC defines the utc timestamp format in log files
	LogTimestampFormatUTC = "2006-01-02T15:04:05.000Z"
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
