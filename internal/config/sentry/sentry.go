package sentry

import (
	"fmt"

	raven "github.com/getsentry/raven-go"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/panichandler"
)

// Config contains configuration for sentry
type Config struct {
	DSN         string `toml:"sentry_dsn"`
	Environment string `toml:"sentry_environment"`
}

// ConfigureSentry configures the sentry DSN
func ConfigureSentry(version string, sentryConf Config) {
	if sentryConf.DSN == "" {
		return
	}

	log.Debug("Using sentry logging")
	raven.SetDSN(sentryConf.DSN)
	if version != "" {
		raven.SetRelease("v" + version)
	}

	if sentryConf.Environment != "" {
		raven.SetEnvironment(sentryConf.Environment)
	}

	panichandler.InstallPanicHandler(func(grpcMethod string, _err interface{}) {
		err, ok := _err.(error)
		if !ok {
			err = fmt.Errorf("%v", _err)
		}

		raven.CaptureError(err, map[string]string{
			"grpcMethod": grpcMethod,
			"panic":      "1",
		})
	})
}
