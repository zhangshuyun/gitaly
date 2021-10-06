package backup

import (
	"testing"

	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestMain(m *testing.M) {
	// gocloud.dev/blob leaks the HTTP connection even if we make sure to close all buckets.
	//nolint:staticcheck
	testhelper.Run(m, testhelper.WithDisabledGoroutineChecker())
}
