// +build static,system_libgit2

package commit

import (
	"os"

	git "github.com/libgit2/git2go/v31"
	"gitlab.com/gitlab-org/gitaly/internal/git2go"
)

func validateFileExists(index *git.Index, path string) error {
	if _, err := index.Find(path); err != nil {
		if git.IsErrorCode(err, git.ErrNotFound) {
			return git2go.FileNotFoundError(path)
		}

		return err
	}

	return nil
}

func validateFileDoesNotExist(index *git.Index, path string) error {
	_, err := index.Find(path)
	if err == nil {
		return git2go.FileExistsError(path)
	}

	if !git.IsErrorCode(err, git.ErrNotFound) {
		return err
	}

	return nil
}

func validateDirectoryDoesNotExist(index *git.Index, path string) error {
	_, err := index.FindPrefix(path + string(os.PathSeparator))
	if err == nil {
		return git2go.DirectoryExistsError(path)
	}

	if !git.IsErrorCode(err, git.ErrNotFound) {
		return err
	}

	return nil
}
