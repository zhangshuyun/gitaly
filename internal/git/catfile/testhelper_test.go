package catfile

import (
	"os"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
)

func TestMain(m *testing.M) {
	testhelper.Configure()
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	return m.Run()
}
