package log

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
)

// HookLogger is a wrapper around *logrus.Logger
type HookLogger struct {
	logger *logrus.Logger
}

// NewHookLogger creates a file logger, since both stderr and stdout will be displayed in git output
func NewHookLogger() *HookLogger {
	logger := logrus.New()

	logDir := os.Getenv(GitalyLogDirEnvKey)
	if logDir == "" {
		logger.SetOutput(io.Discard)
		return &HookLogger{logger: logger}
	}

	logFile, err := os.OpenFile(filepath.Join(logDir, "gitaly_hooks.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		logger.SetOutput(io.Discard)
	} else {
		logger.SetOutput(logFile)
	}

	return &HookLogger{logger: logger}
}

// Fatal logs an error at the Fatal level and writes a generic message to stderr
func (h *HookLogger) Fatal(err error) {
	h.Fatalf("%v", err)
}

// Fatalf logs a formatted error at the Fatal level
func (h *HookLogger) Fatalf(format string, a ...interface{}) {
	fmt.Fprintln(os.Stderr, "error executing git hook")
	h.logger.Fatalf(format, a...)
}

// Errorf logs a formatted error at the Fatal level
func (h *HookLogger) Errorf(format string, a ...interface{}) {
	h.logger.Errorf(format, a...)
}

// Logger returns the underlying logrus logger
func (h *HookLogger) Logger() *logrus.Logger { return h.logger }
