package git

import (
	"context"
	"errors"
)

var (
	// ErrReferenceNotFound represents an error when a reference was not
	// found.
	ErrReferenceNotFound = errors.New("reference not found")
	// ErrReferenceAmbiguous represents an error when a reference couldn't
	// unambiguously be resolved.
	ErrReferenceAmbiguous = errors.New("reference is ambiguous")

	// ErrAlreadyExists represents an error when the resource is already exists.
	ErrAlreadyExists = errors.New("already exists")
	// ErrNotFound represents an error when the resource can't be found.
	ErrNotFound = errors.New("not found")
)

// Repository is the common interface of different repository implementations.
type Repository interface {
	// ResolveRevision tries to resolve the given revision to its object
	// ID. This uses the typical DWIM mechanism of git, see gitrevisions(1)
	// for accepted syntax. This will not verify whether the object ID
	// exists. To do so, you can peel the reference to a given object type,
	// e.g. by passing `refs/heads/master^{commit}`.
	ResolveRevision(ctx context.Context, revision Revision) (ObjectID, error)
	// HasBranches returns whether the repository has branches.
	HasBranches(ctx context.Context) (bool, error)
}
