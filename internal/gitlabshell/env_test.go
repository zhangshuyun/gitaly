package gitlabshell_test

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitlabshell"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestGitHooksConfig(t *testing.T) {
	testhelper.ConfigureRuby()

	defer func(cfg config.Cfg) {
		config.Config = cfg
	}(config.Config)

	loggingDir, err := ioutil.TempDir("", t.Name())
	require.NoError(t, err)
	defer func() { os.RemoveAll(loggingDir) }()

	config.Config.Logging.Dir = loggingDir
	config.Config.Logging.Level = "fatal"
	config.Config.Logging.Format = "my-custom-format"
	config.Config.GitlabShell.Dir = "../../ruby/gitlab-shell"
	config.Config.Hooks.CustomHooksDir = "/path/to/custom_hooks"
	config.Config.Gitlab = config.Gitlab{
		URL: "http://gitlaburl.com",
		HTTPSettings: config.HTTPSettings{
			ReadTimeout: 100,
			User:        "user_name",
			Password:    "pwpw",
			CAFile:      "/ca_file_path",
			CAPath:      "/ca_path",
			SelfSigned:  true,
		},
		SecretFile: "secret_file_path",
	}

	dumpConfigPath := filepath.Join(config.Config.Ruby.Dir, "gitlab-shell", "bin", "dump-config")

	var stdout bytes.Buffer

	cmd := exec.Command(dumpConfigPath)
	gitlabshellEnv, err := gitlabshell.Env()
	require.NoError(t, err)
	cmd.Env = append(os.Environ(), gitlabshellEnv...)
	cmd.Stdout = &stdout
	cmd.Stderr = os.Stderr

	require.NoError(t, cmd.Run())

	rubyConfigMap := make(map[string]interface{})

	require.NoError(t, json.NewDecoder(&stdout).Decode(&rubyConfigMap))
	require.Equal(t, config.Config.Logging.Level, rubyConfigMap["log_level"])
	require.Equal(t, config.Config.Logging.Format, rubyConfigMap["log_format"])
	require.Equal(t, config.Config.Gitlab.SecretFile, rubyConfigMap["secret_file"])
	require.Equal(t, config.Config.Hooks.CustomHooksDir, rubyConfigMap["custom_hooks_dir"])
	require.Equal(t, config.Config.Gitlab.URL, rubyConfigMap["gitlab_url"])
	require.Equal(t, config.Config.GitlabShell.Dir, rubyConfigMap["gitlab_shell_dir"])

	// HTTP Settings
	httpSettings, ok := rubyConfigMap["http_settings"].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, float64(config.Config.Gitlab.HTTPSettings.ReadTimeout), httpSettings["read_timeout"])
	require.Equal(t, config.Config.Gitlab.HTTPSettings.User, httpSettings["user"])
	require.Equal(t, config.Config.Gitlab.HTTPSettings.Password, httpSettings["password"])
	require.Equal(t, config.Config.Gitlab.HTTPSettings.CAFile, httpSettings["ca_file"])
	require.Equal(t, config.Config.Gitlab.HTTPSettings.CAPath, httpSettings["ca_path"])
	require.Equal(t, config.Config.Gitlab.HTTPSettings.SelfSigned, httpSettings["self_signed_cert"])

	dir := filepath.Dir(rubyConfigMap["log_file"].(string))
	require.Equal(t, config.Config.Logging.Dir, dir)
}
