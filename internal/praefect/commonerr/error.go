// Package commonerr contains common errors between different Praefect components. Ideally
// this package's contents would be in praefect's root package but this is not possible at the moment
// due to cyclic imports.
package commonerr

import (
	"errors"
	"fmt"
)

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

// ErrRepositoryNotFound is returned when operating on a repository that doesn't exist.
//
// This somewhat duplicates the above RepositoryNotFoundError but doesn't specify which repository was not found.
// With repository IDs in use, the virtual storage and relative path won't be available everywhere anymore.
var ErrRepositoryNotFound = errors.New("repository not found")

// ErrRepositoryAlreadyExists is returned when attempting to create a repository that already exists.
var ErrRepositoryAlreadyExists = errors.New("repository already exists")
