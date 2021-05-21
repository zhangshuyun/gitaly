package gittest

import (
	"strings"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
)

// RemoteExists tests if the repository at repoPath has a Git remote named remoteName.
func RemoteExists(t testing.TB, cfg config.Cfg, repoPath string, remoteName string) bool {
	if remoteName == "" {
		t.Fatal("empty remote name")
	}

	remotes := Exec(t, cfg, "-C", repoPath, "remote")
	for _, r := range strings.Split(string(remotes), "\n") {
		if r == remoteName {
			return true
		}
	}

	return false
}
