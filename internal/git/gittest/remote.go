package gittest

import (
	"strings"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

// RemoteExists tests if the repository at repoPath has a Git remote named remoteName.
func RemoteExists(t testing.TB, repoPath string, remoteName string) bool {
	if remoteName == "" {
		t.Fatal("empty remote name")
	}

	remotes := testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "remote")
	for _, r := range strings.Split(string(remotes), "\n") {
		if r == remoteName {
			return true
		}
	}

	return false
}
