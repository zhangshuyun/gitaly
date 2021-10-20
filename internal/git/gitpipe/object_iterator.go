package gitpipe

import "gitlab.com/gitlab-org/gitaly/v14/internal/git"

// ObjectIterator is a common interface that is shared across the pipeline steps that work with
// objects.
type ObjectIterator interface {
	// Next iterates to the next item. Returns `false` in case there are no more results left,
	// or if an error happened during iteration. The caller must call `Err()` after `Next()` has
	// returned `false`.
	Next() bool
	// Err returns the first error that was encountered.
	Err() error
	// ObjectID returns the object ID of the current object.
	ObjectID() git.ObjectID
	// ObjectName returns the object name of the current object. This is a
	// implementation-specific field and may not be set.
	ObjectName() []byte
}
