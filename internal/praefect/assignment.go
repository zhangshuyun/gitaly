package praefect

import (
	"context"
	"errors"
)

// AssignmentStore is the interface which Praefect uses to operate on repository assignments.
type AssignmentStore interface {
	AssignmentGetter
	// SetReplicationFactor sets a repository's replication factor to the desired value and returns the
	// resulting assignments.
	SetReplicationFactor(ctx context.Context, virtualStorage, relativePath string, replicationFactor int) ([]string, error)
}

type disabledAssignments map[string][]string

// NewDisabledAssignmentStore returns an assignments store that can be used if no
// database is configured. It returns every configured storage as assigned and
// errors when trying to set assignments.
func NewDisabledAssignmentStore(storages map[string][]string) AssignmentStore {
	return disabledAssignments(storages)
}

// GetHostAssigments simply returns all of the storages configured for the virtual storage.
func (storages disabledAssignments) GetHostAssignments(ctx context.Context, virtualStorage string, repositoryID int64) ([]string, error) {
	return storages[virtualStorage], nil
}

// SetReplicationFactor errors when attempting to set assignments.
func (disabledAssignments) SetReplicationFactor(context.Context, string, string, int) ([]string, error) {
	return nil, errors.New("assignments are disabled")
}
