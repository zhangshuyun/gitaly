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

// EnvFromConfig returns a set of environment variables from a config struct relevant to gitlab shell
func EnvFromConfig(cfg config.Cfg) ([]string, error) {
	fmt.Printf("\n GITLAB SHELL DIR FROM ENV: %v\n", cfg.GitlabShell.Dir)
	gitlabShellConfig := Config{
		CustomHooksDir: cfg.Hooks.CustomHooksDir,
		GitlabURL:      cfg.Gitlab.URL,
		HTTPSettings:   cfg.Gitlab.HTTPSettings,
		LogFormat:      cfg.Logging.Format,
		LogLevel:       cfg.Logging.Level,
		LogPath:        cfg.Logging.Dir,
		RootPath:       cfg.GitlabShell.Dir, //GITLAB_SHELL_DIR has been deprecated
		SecretFile:     cfg.Gitlab.SecretFile,
	}

	gitlabShellConfigString, err := json.Marshal(&gitlabShellConfig)
	if err != nil {
		return nil, err
	}

	return []string{
		//TODO: remove GITALY_GITLAB_SHELL_DIR: https://gitlab.com/gitlab-org/gitaly/-/issues/2679
		"GITALY_GITLAB_SHELL_DIR=" + cfg.GitlabShell.Dir,
		"GITALY_LOG_DIR=" + cfg.Logging.Dir,
		"GITALY_LOG_FORMAT=" + cfg.Logging.Format,
		"GITALY_LOG_LEVEL=" + cfg.Logging.Level,
		"GITALY_BIN_DIR=" + cfg.BinDir,
		"GITALY_RUBY_DIR=" + cfg.Ruby.Dir,
		"GITALY_GITLAB_SHELL_CONFIG=" + string(gitlabShellConfigString),
	}, nil
}
