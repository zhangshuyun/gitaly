package localrepo

import (
	"os"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
)

// Path returns the on-disk path of the repository.
func (repo *Repo) Path() (string, error) {
	return repo.locator.GetRepoPath(repo)
}

// ObjectDirectoryPath returns the full path of the object directory. The errors returned are gRPC
// errors with relevant error codes and should be passed back to gRPC without further decoration.
func (repo *Repo) ObjectDirectoryPath() (string, error) {
	repoPath, err := repo.Path()
	if err != nil {
		return "", err
	}

	objectDirectoryPath := repo.GetGitObjectDirectory()
	if objectDirectoryPath == "" {
		return "", helper.ErrInvalidArgumentf("object directory path is not set")
	}

	// We need to check whether the relative object directory as given by the repository is
	// a valid path. This may either be a path in the Git repository itself, where it may either
	// point to the main object directory storage or to an object quarantine directory as
	// created by git-receive-pack(1). Alternatively, if that is not the case, then it may be a
	// manual object quarantine directory located in the storage's temporary directory. These
	// have a repository-specific prefix which we must check in order to determine whether the
	// quarantine directory does in fact belong to the repo at hand.
	if _, origError := storage.ValidateRelativePath(repoPath, objectDirectoryPath); origError != nil {
		tempDir, err := repo.locator.TempDir(repo.GetStorageName())
		if err != nil {
			return "", helper.ErrInvalidArgumentf("getting storage's temporary directory: %s", err)
		}

		expectedQuarantinePrefix := filepath.Join(tempDir, storage.QuarantineDirectoryPrefix(repo))
		absoluteObjectDirectoryPath := filepath.Join(repoPath, objectDirectoryPath)

		if !strings.HasPrefix(absoluteObjectDirectoryPath, expectedQuarantinePrefix) {
			return "", helper.ErrInvalidArgumentf("not a valid relative path: %s", origError)
		}
	}

	fullPath := filepath.Join(repoPath, objectDirectoryPath)
	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		return "", helper.ErrNotFoundf("object directory does not exist: %q", fullPath)
	}

	return fullPath, nil
}

// InfoAlternatesPath returns the full path of the alternates file.
func (repo *Repo) InfoAlternatesPath() (string, error) {
	repoPath, err := repo.Path()
	if err != nil {
		return "", err
	}

	return filepath.Join(repoPath, "objects", "info", "alternates"), nil
}
