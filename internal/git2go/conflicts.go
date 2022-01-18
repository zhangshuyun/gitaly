package git2go

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ConflictsCommand contains parameters to perform a merge and return its conflicts.
type ConflictsCommand struct {
	// Repository is the path to execute merge in.
	Repository string
	// Ours is the commit that is to be merged into theirs.
	Ours string
	// Theirs is the commit into which ours is to be merged.
	Theirs string
}

// ConflictEntry represents a conflict entry which is one of the sides of a conflict.
type ConflictEntry struct {
	// Path is the path of the conflicting file.
	Path string
	// Mode is the mode of the conflicting file.
	Mode int32
}

// Conflict represents a merge conflict for a single file.
type Conflict struct {
	// Ancestor is the conflict entry of the merge-base.
	Ancestor ConflictEntry
	// Our is the conflict entry of ours.
	Our ConflictEntry
	// Their is the conflict entry of theirs.
	Their ConflictEntry
	// Content contains the conflicting merge results.
	Content []byte
}

// ConflictError is an error which happened during conflict resolution.
type ConflictError struct {
	// Code is the GRPC error code
	Code codes.Code
	// Message is the error message
	Message string
}

func (e ConflictError) Error() string {
	return status.Error(e.Code, e.Message).Error()
}

// ConflictsResult contains all conflicts resulting from a merge.
type ConflictsResult struct {
	// Conflicts
	Conflicts []Conflict
	// Error is an optional conflict error
	//
	// Deprecated: Use Err instead. Error clashes with the Result error field.
	Error ConflictError
	// Err is set if an error occurred. Err must exist on all gob serialized
	// results so that any error can be returned.
	//
	// Will be a ConflictError when an error occurred with the conflicts
	// subcommand.
	Err error
}

// Conflicts performs a merge via gitaly-git2go and returns all resulting conflicts.
func (b Executor) Conflicts(ctx context.Context, repo repository.GitRepo, c ConflictsCommand) (ConflictsResult, error) {
	if err := c.verify(); err != nil {
		return ConflictsResult{}, fmt.Errorf("conflicts: %w: %s", ErrInvalidArgument, err.Error())
	}

	input := &bytes.Buffer{}
	const cmd = "conflicts"
	if err := gob.NewEncoder(input).Encode(c); err != nil {
		return ConflictsResult{}, fmt.Errorf("%s: %w", cmd, err)
	}

	output, err := b.run(ctx, repo, input, cmd)
	if err != nil {
		return ConflictsResult{}, fmt.Errorf("%s: %w", cmd, err)
	}

	var result ConflictsResult
	if err := gob.NewDecoder(output).Decode(&result); err != nil {
		return ConflictsResult{}, fmt.Errorf("%s: %w", cmd, err)
	}

	var conflictErr ConflictError
	switch {
	case errors.As(result.Err, &conflictErr):
		return ConflictsResult{}, status.Error(conflictErr.Code, conflictErr.Message)
	case result.Err != nil:
		return ConflictsResult{}, result.Err
	}

	if result.Error.Code != codes.OK {
		return ConflictsResult{}, status.Error(result.Error.Code, result.Error.Message)
	}
	return result, nil
}

func (c ConflictsCommand) verify() error {
	if c.Repository == "" {
		return errors.New("missing repository")
	}
	if c.Ours == "" {
		return errors.New("missing ours")
	}
	if c.Theirs == "" {
		return errors.New("missing theirs")
	}
	return nil
}
