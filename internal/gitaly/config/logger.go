package config

import (
	gitalylog "gitlab.com/gitlab-org/gitaly/v13/internal/log"
)

// ConfigureLogging uses the global conf and environmental vars to configure log levels and format
func ConfigureLogging() {
	gitalylog.Configure(Config.Logging.Format, Config.Logging.Level)
}
