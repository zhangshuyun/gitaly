package testhelper

import "testing"

// CreateRemoteBranch creates a new remote branch
func CreateRemoteBranch(t testing.TB, gitBin, repoPath, remoteName, branchName, ref string) {
	MustRunCommand(t, nil, gitBin, "-C", repoPath, "update-ref",
		"refs/remotes/"+remoteName+"/"+branchName, ref)
}
