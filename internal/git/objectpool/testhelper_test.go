package objectpool

import (
	"testing"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestMain(m *testing.M) {
	testhelper.Run(m, testhelper.WithSetup(func() error {
		hooks.Override = "/"
		return nil
	}))
}
