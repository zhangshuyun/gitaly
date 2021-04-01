// Package commonerr contains common errors between different Praefect components. Ideally
// this package's contents would be in praefect's root package but this is not possible at the moment
// due to cyclic imports.
package commonerr

import "fmt"

// RepositoryNotFoundError is returned when attempting to operate on a repository
// that does not exist in the virtual storage.
type RepositoryNotFoundError struct {
	virtualStorage string
	relativePath   string
}

// NewRepositoryNotFoundError returns a new repository not found error for the given repository.
func NewRepositoryNotFoundError(virtualStorage string, relativePath string) error {
	return RepositoryNotFoundError{virtualStorage: virtualStorage, relativePath: relativePath}
}

// Error returns the error message.
func (err RepositoryNotFoundError) Error() string {
	return fmt.Sprintf("repository %q/%q not found", err.virtualStorage, err.relativePath)
}
