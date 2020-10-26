// +build static,system_libgit2

package command

import (
	"os"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestMain(m *testing.M) {
	defer testhelper.MustHaveNoChildProcess()
	testhelper.Configure()
	testhelper.ConfigureGitalyGit2Go()
	os.Exit(m.Run())
}
