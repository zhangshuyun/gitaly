package log

import (
	"fmt"
	"io/ioutil"
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

	filepath := filepath.Join(os.Getenv("GITALY_LOG_DIR"), "gitaly_hooks.log")

	logFile, err := os.OpenFile(filepath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		logger.SetOutput(ioutil.Discard)
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
	fmt.Fprintf(os.Stderr, "error executing git hook")
	fmt.Fprintf(os.Stderr, format, a...)
	h.logger.Fatalf(format, a...)
}

// Fatalf logs a formatted error at the Fatal level
func (h *HookLogger) Errorf(format string, a ...interface{}) {
	h.logger.Errorf(format, a...)
}
