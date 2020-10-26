package git2go_test

import (
	"os"
	"path/filepath"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

var executors = map[string]git2go.Executor{}

func TestMain(m *testing.M) {
	testhelper.Configure()
	testhelper.ConfigureGitalyGit2Go()

	executors["subprocess"] = git2go.New(filepath.Join(config.Config.BinDir, "gitaly-git2go"))

	os.Exit(m.Run())
}

// testExecutor is a helper for running a test with a subprocess and in-process.
func testExecutors(t *testing.T, test func(*testing.T, git2go.Executor)) {
	for name, executor := range executors {
		t.Run(name, func(t *testing.T) { test(t, executor) })
	}
}
