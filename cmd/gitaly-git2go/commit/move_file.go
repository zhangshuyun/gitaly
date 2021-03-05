// +build static,system_libgit2

package commit

import (
	git "github.com/libgit2/git2go/v31"
	"gitlab.com/gitlab-org/gitaly/internal/git2go"
)

func applyMoveFile(action git2go.MoveFile, index *git.Index) error {
	entry, err := index.EntryByPath(action.Path, 0)
	if err != nil {
		if git.IsErrorCode(err, git.ErrNotFound) {
			return git2go.FileNotFoundError(action.Path)
		}

		return err
	}

	if err := validateFileDoesNotExist(index, action.NewPath); err != nil {
		return err
	}

	oid := entry.Id
	if action.OID != "" {
		oid, err = git.NewOid(action.OID)
		if err != nil {
			return err
		}
	}

	if err := index.Add(&git.IndexEntry{
		Path: action.NewPath,
		Mode: entry.Mode,
		Id:   oid,
	}); err != nil {
		return err
	}

	return index.RemoveByPath(entry.Path)
}
