package git2go

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"reflect"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
)

func init() {
	for typee := range registeredTypes {
		gob.Register(typee)
	}
}

var registeredTypes = map[interface{}]struct{}{
	ChangeFileMode{}:         {},
	CreateDirectory{}:        {},
	CreateFile{}:             {},
	DeleteFile{}:             {},
	MoveFile{}:               {},
	UpdateFile{}:             {},
	wrapError{}:              {},
	DirectoryExistsError(""): {},
	FileExistsError(""):      {},
	FileNotFoundError(""):    {},
	InvalidArgumentError(""): {},
	HasConflictsError{}:      {},
	IndexError(""):           {},
}

// Result is the serialized result.
type Result struct {
	// CommitID is the result of the call.
	CommitID string
	// Error is set if the call errord.
	Error error
}

// wrapError is used to serialize wrapped errors as fmt.wrapError type only has
// private fields and can't be serialized via gob. It's also used to serialize unregistered
// error types by serializing only their error message.
type wrapError struct {
	Message string
	Err     error
}

func (err wrapError) Error() string { return err.Message }

func (err wrapError) Unwrap() error { return err.Err }

// HasConflictsError is used when a change, for example a revert, could not be
// applied due to a conflict.
type HasConflictsError struct{}

func (err HasConflictsError) Error() string {
	return "could not apply due to conflicts"
}

// SerializableError returns an error that is Gob serializable.
// Registered types are serialized directly. Unregistered types
// are transformed in to an opaque error using their error message.
// Wrapped errors remain unwrappable.
func SerializableError(err error) error {
	if err == nil {
		return nil
	}

	if unwrappedErr := errors.Unwrap(err); unwrappedErr != nil {
		return wrapError{
			Message: err.Error(),
			Err:     SerializableError(unwrappedErr),
		}
	}

	if _, ok := registeredTypes[reflect.Zero(reflect.TypeOf(err)).Interface()]; !ok {
		return wrapError{Message: err.Error()}
	}

	return err
}

// runWithGob runs the specified gitaly-git2go cmd with the request gob-encoded
// as input and returns the commit ID as string or an error.
func runWithGob(ctx context.Context, cfg config.Cfg, cmd string, request interface{}) (git.ObjectID, error) {
	input := &bytes.Buffer{}
	if err := gob.NewEncoder(input).Encode(request); err != nil {
		return "", fmt.Errorf("%s: %w", cmd, err)
	}

	output, err := run(ctx, binaryPathFromCfg(cfg), input, cmd)
	if err != nil {
		return "", fmt.Errorf("%s: %w", cmd, err)
	}

	var result Result
	if err := gob.NewDecoder(output).Decode(&result); err != nil {
		return "", fmt.Errorf("%s: %w", cmd, err)
	}

	if result.Error != nil {
		return "", fmt.Errorf("%s: %w", cmd, result.Error)
	}

	commitID, err := git.NewObjectIDFromHex(result.CommitID)
	if err != nil {
		return "", fmt.Errorf("could not parse commit ID: %w", err)
	}

	return commitID, nil
}
