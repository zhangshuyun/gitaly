package log

import (
	"bytes"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
