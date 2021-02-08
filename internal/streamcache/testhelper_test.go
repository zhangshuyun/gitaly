package streamcache

import (
	"os"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestMain(m *testing.M) { os.Exit(testMain(m)) }

func testMain(m *testing.M) int {
	defer testhelper.Configure()()
	return m.Run()
}
