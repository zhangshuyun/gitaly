package testhelper

import (
	"io/ioutil"
	"testing"

	log "github.com/sirupsen/logrus"
)

type testHook struct {
	t         testing.TB
	formatter log.Formatter
}

func (s testHook) Levels() []log.Level {
	return []log.Level{
		log.DebugLevel,
		log.InfoLevel,
		log.WarnLevel,
		log.ErrorLevel,
		log.FatalLevel,
		log.PanicLevel,
	}
}

func (s testHook) Fire(entry *log.Entry) error {
	formatted, err := s.formatter.Format(entry)
	if err != nil {
		return err
	}

	formattedString := string(formatted)

	switch entry.Level {
	case log.FatalLevel, log.PanicLevel:
		s.t.Fatal(formattedString)

	default:
		s.t.Log(formattedString)
	}

	return nil
}

// NewTestLogger created a logrus hook which can be used with testing logs
func NewTestLogger(t testing.TB) *log.Logger {
	logger := log.New()
	logger.Out = ioutil.Discard
	formatter := &log.TextFormatter{}

	logger.Hooks.Add(testHook{t, formatter})

	return logger
}
