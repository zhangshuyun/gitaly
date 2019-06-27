package log

import (
	"os"
	"runtime"

	"github.com/sirupsen/logrus"
)

// NewHookLogger creates a file logger, since both stderr and stdout will be displayed in git output
func NewHookLogger(filepath string) (*logrus.Logger, error) {
	logger := logrus.New()

	logFile, err := os.OpenFile(filepath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	logger.SetOutput(logFile)

	runtime.SetFinalizer(logFile, func(f *os.File) {
		f.Close()
	})

	return logger, nil
}
