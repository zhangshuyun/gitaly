package git2go

import (
	"context"
	"errors"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ConflictsCommand contains parameters to perform a merge and return its conflicts.
type ConflictsCommand struct {
	// Repository is the path to execute merge in.
	Repository string `json:"repository"`
	// Ours is the commit that is to be merged into theirs.
	Ours string `json:"ours"`
	// Theirs is the commit into which ours is to be merged.
	Theirs string `json:"theirs"`
}

// ConflictEntry represents a conflict entry which is one of the sides of a conflict.
type ConflictEntry struct {
	// Path is the path of the conflicting file.
	Path string `json:"path"`
	// Mode is the mode of the conflicting file.
	Mode int32 `json:"mode"`
}

// Conflict represents a merge conflict for a single file.
type Conflict struct {
	// Ancestor is the conflict entry of the merge-base.
	Ancestor ConflictEntry `json:"ancestor"`
	// Our is the conflict entry of ours.
	Our ConflictEntry `json:"our"`
	// Their is the conflict entry of theirs.
	Their ConflictEntry `json:"their"`
	// Content contains the conflicting merge results.
	Content []byte `json:"content"`
}

type ConflictError struct {
	// Code is the GRPC error code
	Code codes.Code
	// Message is the error message
	Message string
}

// ConflictsResult contains all conflicts resulting from a merge.
type ConflictsResult struct {
	// Conflicts
	Conflicts []Conflict `json:"conflicts"`
	// Error is an optional conflict error
	Error ConflictError `json:"error"`
}

// ConflictsCommandFromSerialized constructs a ConflictsCommand from its serialized representation.
func ConflictsCommandFromSerialized(serialized string) (ConflictsCommand, error) {
	var request ConflictsCommand
	if err := deserialize(serialized, &request); err != nil {
		return ConflictsCommand{}, err
	}

	if err := request.verify(); err != nil {
		return ConflictsCommand{}, fmt.Errorf("conflicts: %w: %s", ErrInvalidArgument, err.Error())
	}

	return request, nil
}

// SerializeTo serializes the conflicts result and writes it into the writer.
func (m ConflictsResult) SerializeTo(writer io.Writer) error {
	return serializeTo(writer, m)
}

// Run performs a merge via gitaly-git2go and returns all resulting conflicts.
func (c ConflictsCommand) Run(ctx context.Context, cfg config.Cfg) (ConflictsResult, error) {
	if err := c.verify(); err != nil {
		return ConflictsResult{}, fmt.Errorf("conflicts: %w: %s", ErrInvalidArgument, err.Error())
	}

	serialized, err := serialize(c)
	if err != nil {
		return ConflictsResult{}, err
	}

	stdout, err := run(ctx, BinaryPath(cfg.BinDir), nil, "conflicts", "-request", serialized)
	if err != nil {
		return ConflictsResult{}, err
	}

	var response ConflictsResult
	if err := deserialize(stdout.String(), &response); err != nil {
		return ConflictsResult{}, err
	}

	if response.Error.Code != codes.OK {
		return ConflictsResult{}, status.Error(response.Error.Code, response.Error.Message)
	}

	return response, nil
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
