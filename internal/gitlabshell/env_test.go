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

	dumpConfigPath := filepath.Join(config.Config.Ruby.Dir, "gitlab-shell", "bin", "dump-config")

	var stdout bytes.Buffer

	cmd := exec.Command(dumpConfigPath)
	cmd.Env = append(os.Environ(), gitlabshell.Env()...)
	cmd.Stdout = &stdout

	require.NoError(t, cmd.Run())

	rubyConfigMap := make(map[string]interface{})

	require.NoError(t, json.NewDecoder(&stdout).Decode(&rubyConfigMap))
	require.Equal(t, config.Config.Logging.Level, rubyConfigMap["log_level"])
	require.Equal(t, config.Config.Logging.Format, rubyConfigMap["log_format"])

	dir := filepath.Dir(rubyConfigMap["log_file"].(string))
	require.Equal(t, config.Config.Logging.Dir, dir)
}
