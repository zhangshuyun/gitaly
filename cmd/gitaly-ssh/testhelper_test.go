package main

import (
	"os"
	"path/filepath"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
)

var gitalySSHPath string

func TestMain(m *testing.M) {
	testhelper.Configure()
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	testhelper.ConfigureGitalySSH()
	gitalySSHPath = filepath.Join(config.Config.BinDir, "gitaly-ssh")

	return m.Run()
}
