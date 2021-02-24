package gittest

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

// EnableGitProtocolV2Support replaces the git binary in config with a wrapper that allows the
// protocol to be tested. It returns a function to read the GIT_PROTOCOl environment variable
// created by the wrapper script, the modified configuration as well as a cleanup function.
func EnableGitProtocolV2Support(t testing.TB, cfg config.Cfg) (func() string, config.Cfg, testhelper.Cleanup) {
	dir, cleanupDir := testhelper.TempDir(t)

	gitPath := filepath.Join(dir, "git")
	envPath := filepath.Join(dir, "git-env")

	script := fmt.Sprintf(`#!/bin/sh
env | grep ^GIT_PROTOCOL= >>"%s"
exec "%s" "$@"
`, envPath, config.Config.Git.BinPath)

	cleanupExe := testhelper.WriteExecutable(t, gitPath, []byte(script))

	cfg.Git.BinPath = gitPath

	return func() string {
			data, err := ioutil.ReadFile(envPath)
			require.NoError(t, err)
			return string(data)
		}, cfg, func() {
			cleanupExe()
			cleanupDir()
		}
}
