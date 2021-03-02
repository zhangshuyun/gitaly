package gittest

import (
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

// CreateRemoteBranch creates a new remote branch
func CreateRemoteBranch(t testing.TB, repoPath, remoteName, branchName, ref string) {
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "update-ref",
		"refs/remotes/"+remoteName+"/"+branchName, ref)
}
