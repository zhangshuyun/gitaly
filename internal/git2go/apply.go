package git2go

import (
	"context"
	"encoding/gob"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/internal/git"
)

// ErrMergeConflict is returned when there is a merge conflict.
var ErrMergeConflict = wrapError{Message: "merge conflict"}

// Patch represents a single patch.
type Patch struct {
	// Author is the author of the patch.
	Author Signature
	// Message is used as the commit message when applying the patch.
	Message string
	// Diff contains the diff of the patch.
	Diff []byte
}

// ApplyParams are the parameters for Apply.
type ApplyParams struct {
	// Repository is the path to the repository.
	Repository string
	// Committer is the committer applying the patch.
	Committer Signature
	// ParentCommit is the OID of the commit to apply the patches against.
	ParentCommit string
	// Patches iterates over all the patches to be applied.
	Patches PatchIterator
}

// PatchIterator iterates over a stream of patches.
type PatchIterator interface {
	// Next returns whether there is a next patch.
	Next() bool
	// Value returns the patch being currently iterated upon.
	Value() Patch
	// Err returns the iteration error. Err should
	// be always checked after Next returns false.
	Err() error
}

type slicePatchIterator struct {
	value   Patch
	patches []Patch
}

// NewSlicePatchIterator returns a PatchIterator that iterates over the slice
// of patches.
func NewSlicePatchIterator(patches []Patch) PatchIterator {
	return &slicePatchIterator{patches: patches}
}

func (iter *slicePatchIterator) Next() bool {
	if len(iter.patches) == 0 {
		return false
	}

	iter.value = iter.patches[0]
	iter.patches = iter.patches[1:]
	return true
}

func (iter *slicePatchIterator) Value() Patch { return iter.value }

func (iter *slicePatchIterator) Err() error { return nil }

// Apply applies the provided patches and returns the OID of the commit with the patches
// applied.
func (b Executor) Apply(ctx context.Context, params ApplyParams) (git.ObjectID, error) {
	reader, writer := io.Pipe()
	defer writer.Close()

	go func() {
		writer.CloseWithError(func() error {
			patches := params.Patches
			params.Patches = nil

			encoder := gob.NewEncoder(writer)
			if err := encoder.Encode(params); err != nil {
				return fmt.Errorf("encode header: %w", err)
			}

			for patches.Next() {
				if err := encoder.Encode(patches.Value()); err != nil {
					return fmt.Errorf("encode patch: %w", err)
				}
			}

			if err := patches.Err(); err != nil {
				return fmt.Errorf("patch iterator: %w", err)
			}

			return nil
		}())
	}()

	var result Result
	output, err := run(ctx, b.binaryPath, reader, "apply", "-git-binary-path", b.gitBinaryPath)
	if err != nil {
		return "", fmt.Errorf("run: %w", err)
	}

	if err := gob.NewDecoder(output).Decode(&result); err != nil {
		return "", fmt.Errorf("decode: %w", err)
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
