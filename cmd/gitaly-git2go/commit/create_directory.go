// +build static,system_libgit2

package commit

import (
	"fmt"
	"path/filepath"

	git "github.com/libgit2/git2go/v31"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git2go"
)

func applyCreateDirectory(action git2go.CreateDirectory, repo *git.Repository, index *git.Index) error {
	if err := validateFileDoesNotExist(index, action.Path); err != nil {
		return err
	} else if err := validateDirectoryDoesNotExist(index, action.Path); err != nil {
		return err
	}

	emptyBlobOID, err := repo.CreateBlobFromBuffer([]byte{})
	if err != nil {
		return fmt.Errorf("create blob from buffer: %w", err)
	}

	return index.Add(&git.IndexEntry{
		Path: filepath.Join(action.Path, ".gitkeep"),
		Mode: git.FilemodeBlob,
		Id:   emptyBlobOID,
	})
}
