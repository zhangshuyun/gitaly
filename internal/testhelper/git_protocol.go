package testhelper

import (
	"gitlab.com/gitlab-org/gitaly/internal/config"
)

// EnableGitProtocolV2Support replaces the git binary in config with an
// `env_git` wrapper that allows the protocol to be tested. It returns a
// function that restores the given settings.
//
// Because we don't know how to get to that wrapper script from the current
// working directory, callers must create a symbolic link to the `env_git` file
// in their own `testdata` directories.
func EnableGitProtocolV2Support() func() {
	oldGitBinPath := config.Config.Git.BinPath
	config.Config.Git.BinPath = "testdata/env_git"
	return func() {
		config.Config.Git.BinPath = oldGitBinPath
	}
}
