package localrepo

// Path returns the on-disk path of the repository.
func (repo *Repo) Path() (string, error) {
	return repo.locator.GetRepoPath(repo)
}

// ObjectDirectoryPath returns the full path of the object directory. The errors returned are gRPC
// errors with relevant error codes and should be passed back to gRPC without further decoration.
func (repo *Repo) ObjectDirectoryPath() (string, error) {
	return repo.locator.GetObjectDirectoryPath(repo)
}

// InfoAlternatesPath returns the full path of the alternates file.
func (repo *Repo) InfoAlternatesPath() (string, error) {
	return repo.locator.InfoAlternatesPath(repo)
}
