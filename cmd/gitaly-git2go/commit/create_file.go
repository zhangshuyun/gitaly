// +build static,system_libgit2

package commit

import (
	git "github.com/libgit2/git2go/v31"
	"gitlab.com/gitlab-org/gitaly/internal/git2go"
)

func applyCreateFile(action git2go.CreateFile, index *git.Index) error {
	if err := validateFileDoesNotExist(index, action.Path); err != nil {
		return err
	}

	oid, err := git.NewOid(action.OID)
	if err != nil {
		return err
	}

	mode := git.FilemodeBlob
	if action.ExecutableMode {
		mode = git.FilemodeBlobExecutable
	}

	return index.Add(&git.IndexEntry{
		Path: action.Path,
		Mode: mode,
		Id:   oid,
	})
}
