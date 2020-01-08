package gitlabshell

import (
	"gitlab.com/gitlab-org/gitaly/internal/config"
)

// Env is a helper that returns a slice with environment variables used by gitlab shell
func Env() []string {
	cfg := config.Config

	return EnvFromConfig(cfg)
}

// EnvFromConfig returns a set of environment variables from a config struct relevant to gitlab shell
func EnvFromConfig(cfg config.Cfg) []string {
	return []string{
		"GITALY_GITLAB_SHELL_DIR=" + cfg.GitlabShell.Dir,
		"GITALY_LOG_DIR=" + cfg.Logging.Dir,
		"GITALY_LOG_FORMAT=" + cfg.Logging.Format,
		"GITALY_LOG_LEVEL=" + cfg.Logging.Level,
		"GITLAB_SHELL_DIR=" + cfg.GitlabShell.Dir, //GITLAB_SHELL_DIR has been deprecated
		"GITALY_BIN_DIR=" + cfg.BinDir,
		"GITALY_RUBY_DIR=" + cfg.Ruby.Dir,
	}
}
