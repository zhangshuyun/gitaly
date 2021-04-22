package gittest

import (
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
)

// CreateRemoteBranch creates a new remote branch
func CreateRemoteBranch(t testing.TB, cfg config.Cfg, repoPath, remoteName, branchName, ref string) {
	Exec(t, cfg, "-C", repoPath, "update-ref", "refs/remotes/"+remoteName+"/"+branchName, ref)
}
