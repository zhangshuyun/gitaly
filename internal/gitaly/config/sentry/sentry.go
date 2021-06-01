package sentry

import (
	"fmt"

	sentry "github.com/getsentry/sentry-go"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/middleware/panichandler"
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

	err := sentry.Init(sentry.ClientOptions{
		Dsn:         sentryConf.DSN,
		Environment: sentryConf.Environment,
		Release:     "v" + version,
	})
	if err != nil {
		log.Warnf("Unable to initialize sentry client: %v", err)
		return
	}

	log.Debug("Using sentry logging")

	panichandler.InstallPanicHandler(func(grpcMethod string, _err interface{}) {
		err, ok := _err.(error)
		if !ok {
			err = fmt.Errorf("%v", _err)
		}

		sentry.WithScope(func(scope *sentry.Scope) {
			scope.SetTag("grpcMethod", grpcMethod)
			scope.SetTag("panic", "1")
			sentry.CaptureException(err)
		})
	})
}
