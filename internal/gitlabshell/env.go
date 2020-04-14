package gitlabshell

import (
	"encoding/json"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/internal/config"
)

// Env is a helper that returns a slice with environment variables used by gitlab shell
func Env() ([]string, error) {
	cfg := config.Config

	return EnvFromConfig(cfg)
}

// EnvFromConfig returns a set of environment variables from a config struct relevant to gitlab shell
func EnvFromConfig(cfg config.Cfg) ([]string, error) {
	gitlabShellConfig := Config{
		CustomHooksDir: cfg.GitlabShell.CustomHooksDir,
		GitlabURL:      cfg.GitlabShell.GitlabURL,
		HTTPSettings:   cfg.GitlabShell.HTTPSettings,
		LogFormat:      cfg.Logging.Format,
		LogLevel:       cfg.Logging.Level,
		LogPath:        cfg.Logging.Dir,
		RootPath:       cfg.GitlabShell.Dir, //GITLAB_SHELL_DIR has been deprecated
		SecretFile:     cfg.GitlabShell.SecretFile,
	}

	gitlabShellConfigString, err := json.Marshal(&gitlabShellConfig)
	if err != nil {
		return nil, err
	}

	return []string{
		"GITALY_GITLAB_SHELL_DIR=" + cfg.GitlabShell.Dir,
		"GITALY_BIN_DIR=" + cfg.BinDir,
		"GITALY_RUBY_DIR=" + cfg.Ruby.Dir,
		fmt.Sprintf("GITALY_GITLAB_SHELL_CONFIG=%s", gitlabShellConfigString),
	}, nil
}

type Config struct {
	CustomHooksDir string              `json:"custom_hooks_dir"`
	GitlabURL      string              `json:"gitlab_url"`
	HTTPSettings   config.HTTPSettings `json:"http_settings"`
	LogFormat      string              `json:"log_format"`
	LogLevel       string              `json:"log_level"`
	LogPath        string              `json:"log_path"`
	RootPath       string              `json:"root_path"`
	SecretFile     string              `json:"secret_file"`
}
