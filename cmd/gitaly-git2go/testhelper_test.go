//go:build static && system_libgit2
// +build static,system_libgit2

package main

import (
	"testing"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func buildExecutor(cfg config.Cfg) git2go.Executor {
	return git2go.NewExecutor(cfg, git.NewExecCommandFactory(cfg), config.NewLocator(cfg))
}
