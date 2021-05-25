package streamcache

import (
	"os"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestMain(m *testing.M) { os.Exit(testMain(m)) }

func testMain(m *testing.M) int {
	defer testhelper.Configure()()
	return m.Run()
}
