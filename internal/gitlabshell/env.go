package gitlabshell

import "gitlab.com/gitlab-org/gitaly/internal/config"

// Env is a helper that returns a slice with environment variables used by gitlab shell
func Env() []string {
	cfg := config.Config

	return []string{
		"GITALY_GITLAB_SHELL_DIR=" + cfg.GitlabShell.Dir,
		"GITALY_LOG_DIR=" + cfg.Logging.Dir,
		"GITALY_LOG_FORMAT=" + cfg.Logging.Format,
		"GITALY_LOG_LEVEL=" + cfg.Logging.Level,
	}
}
