package testhelper

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v13/internal/command"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/config"
)

// EnableGitProtocolV2Support replaces the git binary in config with a
// wrapper that allows the protocol to be tested. It returns a function that
// restores the given settings as well as an array of environment variables
// which need to be set when invoking Git with this setup.
func EnableGitProtocolV2Support(t *testing.T) func() {
	script := fmt.Sprintf(`#!/bin/sh
mkdir -p testdata
env | grep ^GIT_PROTOCOL= >>testdata/git-env
exec "%s" "$@"
`, command.GitPath())

	dir, err := ioutil.TempDir("", "gitaly-test-*")
	require.NoError(t, err)

	path := filepath.Join(dir, "git")

	cleanup, err := WriteExecutable(path, []byte(script))
	require.NoError(t, err)

	oldGitBinPath := config.Config.Git.BinPath
	config.Config.Git.BinPath = path
	return func() {
		os.Remove("testdata/git-env")
		config.Config.Git.BinPath = oldGitBinPath
		cleanup()
	}
}
