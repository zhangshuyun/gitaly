package bootstrap

import (
	"os"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	cleanup := testhelper.Configure()
	defer cleanup()
	return m.Run()
}
