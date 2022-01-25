package git2goutil

import (
	git "github.com/libgit2/git2go/v33"
)

// OpenRepository opens the repository located at path as a Git2Go repository.
func OpenRepository(path string) (*git.Repository, error) {
	return git.OpenRepositoryExtended(path, git.RepositoryOpenFromEnv, "")
}
