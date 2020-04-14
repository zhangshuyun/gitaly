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
	config.Config.GitlabShell.SecretFile = "secret_file.very_secret"
	config.Config.GitlabShell.CustomHooksDir = "custom_hooks_directory/is/here"
	config.Config.GitlabShell.GitlabURL = "http://localhost:1234"

	dumpConfigPath := filepath.Join(config.Config.Ruby.Dir, "gitlab-shell", "bin", "dump-config")

	var stdout bytes.Buffer

	cmd := exec.Command(dumpConfigPath)
	gitlabshellEnv, err := gitlabshell.Env()
	require.NoError(t, err)
	cmd.Env = append(os.Environ(), gitlabshellEnv...)
	cmd.Stdout = &stdout

	require.NoError(t, cmd.Run())

	rubyConfigMap := make(map[string]interface{})

	require.NoError(t, json.NewDecoder(&stdout).Decode(&rubyConfigMap))
	require.Equal(t, config.Config.Logging.Level, rubyConfigMap["log_level"])
	require.Equal(t, config.Config.Logging.Format, rubyConfigMap["log_format"])
	require.Equal(t, config.Config.GitlabShell.SecretFile, rubyConfigMap["secret_file"])
	require.Equal(t, config.Config.GitlabShell.CustomHooksDir, rubyConfigMap["custom_hooks_dir"])
	require.Equal(t, config.Config.GitlabShell.GitlabURL, rubyConfigMap["gitlab_url"])
	require.Equal(t, config.Config.GitlabShell.SecretFile, rubyConfigMap["secret_file"])

	// HTTP Settings
	httpSettings, ok := rubyConfigMap["http_settings"].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, float64(config.Config.GitlabShell.HTTPSettings.ReadTimeout), httpSettings["read_timeout"])
	require.Equal(t, config.Config.GitlabShell.HTTPSettings.User, httpSettings["user"])
	require.Equal(t, config.Config.GitlabShell.HTTPSettings.Password, httpSettings["password"])
	require.Equal(t, config.Config.GitlabShell.HTTPSettings.CAFile, httpSettings["ca_file"])
	require.Equal(t, config.Config.GitlabShell.HTTPSettings.CAPath, httpSettings["ca_path"])
	require.Equal(t, config.Config.GitlabShell.HTTPSettings.SelfSigned, httpSettings["self_signed_cert"])

	dir := filepath.Dir(rubyConfigMap["log_file"].(string))
	require.Equal(t, config.Config.Logging.Dir, dir)
}
