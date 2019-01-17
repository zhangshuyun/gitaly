package testhelper

import (
	"gitlab.com/gitlab-org/gitaly/internal/config"
)

// EnableGitProtocolV2Support ensures that Git protocol v2 support is enabled,
// and replaces the git binary in config with an `env_git` wrapper that allows
// the protocol to be tested. It returns a function that restores the given
// settings.
//
// Because we don't know how to get to that wrapper script from the current
// working directory, callers must create a symbolic link to the `env_git` file
// in their own `testdata` directories.
func EnableGitProtocolV2Support() func() {
	oldGitBinPath := config.Config.Git.BinPath
	oldGitProtocolV2Enabled := config.Config.Git.ProtocolV2Enabled

	config.Config.Git.BinPath = "testdata/env_git"
	config.Config.Git.ProtocolV2Enabled = true

	return func() {
		config.Config.Git.BinPath = oldGitBinPath
		config.Config.Git.ProtocolV2Enabled = oldGitProtocolV2Enabled
	}
}
