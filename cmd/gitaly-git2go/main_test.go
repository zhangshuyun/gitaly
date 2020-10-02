// +build static,system_libgit2

package main

import (
	"os"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
)

func TestMain(m *testing.M) {
	defer testhelper.MustHaveNoChildProcess()
	testhelper.Configure()
	testhelper.ConfigureGitalyGit2Go()
	os.Exit(m.Run())
}
