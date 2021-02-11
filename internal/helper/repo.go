package helper

import (
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
)

// RepoPathEqual compares if two repositories are in the same location
func RepoPathEqual(a, b repository.GitRepo) bool {
	return a.GetStorageName() == b.GetStorageName() &&
		a.GetRelativePath() == b.GetRelativePath()
}
