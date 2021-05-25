package git2go

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
)

// IndexError is an error that was produced by performing an invalid operation on the index.
type IndexError string

// Error returns the error message of the index error.
func (err IndexError) Error() string { return string(err) }

// InvalidArgumentError is returned when an invalid argument is provided.
type InvalidArgumentError string

func (err InvalidArgumentError) Error() string { return string(err) }

// FileNotFoundError is returned when an action attempts to operate on a non-existing file.
type FileNotFoundError string

func (err FileNotFoundError) Error() string {
	return fmt.Sprintf("file not found: %q", string(err))
}

// FileExistsError is returned when an action attempts to overwrite an existing file.
type FileExistsError string

func (err FileExistsError) Error() string {
	return fmt.Sprintf("file exists: %q", string(err))
}

// DirectoryExistsError is returned when an action attempts to overwrite a directory.
type DirectoryExistsError string

func (err DirectoryExistsError) Error() string {
	return fmt.Sprintf("directory exists: %q", string(err))
}

// CommitParams contains the information and the steps to build a commit.
type CommitParams struct {
	// Repository is the path of the repository to operate on.
	Repository string
	// Author is the author of the commit.
	Author Signature
	// Committer is the committer of the commit.
	Committer Signature
	// Message is message of the commit.
	Message string
	// Parent is the OID of the commit to use as the parent of this commit.
	Parent string
	// Actions are the steps to build the commit.
	Actions []Action
}

// Commit builds a commit from the actions, writes it to the object database and
// returns its object id.
func (b Executor) Commit(ctx context.Context, params CommitParams) (git.ObjectID, error) {
	input := &bytes.Buffer{}
	if err := gob.NewEncoder(input).Encode(params); err != nil {
		return "", err
	}

	output, err := run(ctx, b.binaryPath, input, "commit")
	if err != nil {
		return "", err
	}

	var result Result
	if err := gob.NewDecoder(output).Decode(&result); err != nil {
		return "", err
	}

	if result.Error != nil {
		return "", result.Error
	}

	commitID, err := git.NewObjectIDFromHex(result.CommitID)
	if err != nil {
		return "", fmt.Errorf("could not parse commit ID: %w", err)
	}

	return commitID, nil
}
