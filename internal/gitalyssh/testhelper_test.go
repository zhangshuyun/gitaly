package gitalyssh

import (
	"os"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
)

func TestMain(m *testing.M) {
	testhelper.Configure()
	os.Exit(m.Run())
}
