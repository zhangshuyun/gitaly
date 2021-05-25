package objectpool

import (
	"os"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()
	cleanup := testhelper.Configure()
	defer cleanup()
	hooks.Override = "/"
	return m.Run()
}
