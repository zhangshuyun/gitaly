package testhelper

import (
	"io"
	"testing"

	"github.com/sirupsen/logrus"
)

// NewDiscardingLogger creates a logger that discards everything.
func NewDiscardingLogger(tb testing.TB) *logrus.Logger {
	logger := logrus.New()
	logger.Out = io.Discard
	return logger
}

// NewDiscardingLogEntry creates a logrus entry that discards everything.
func NewDiscardingLogEntry(tb testing.TB) *logrus.Entry {
	return logrus.NewEntry(NewDiscardingLogger(tb))
}
