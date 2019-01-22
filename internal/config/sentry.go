package config

import (
	"fmt"

	raven "github.com/getsentry/raven-go"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/panichandler"
)

// ConfigureSentry configures the sentry DSN
func ConfigureSentry(version string) {
	if Config.Logging.SentryDSN == "" {
		return
	}

	log.Debug("Using sentry logging")
	raven.SetDSN(Config.Logging.SentryDSN)
	if version != "" {
		raven.SetRelease("v" + version)
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
