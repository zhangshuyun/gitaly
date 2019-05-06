package tempdir

import (
	"os"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/command/commandtest"
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer commandtest.MustHaveNoChildProcess()

	return m.Run()
}
