package git2go

import (
	"encoding/gob"
	"errors"
	"reflect"
)

func init() {
	for typeToRegister := range registeredTypes {
		gob.Register(reflect.Zero(typeToRegister).Interface())
	}
}

var registeredTypes = map[reflect.Type]struct{}{
	reflect.TypeOf(ChangeFileMode{}):         {},
	reflect.TypeOf(CreateDirectory{}):        {},
	reflect.TypeOf(CreateFile{}):             {},
	reflect.TypeOf(DeleteFile{}):             {},
	reflect.TypeOf(MoveFile{}):               {},
	reflect.TypeOf(UpdateFile{}):             {},
	reflect.TypeOf(wrapError{}):              {},
	reflect.TypeOf(DirectoryExistsError("")): {},
	reflect.TypeOf(FileExistsError("")):      {},
	reflect.TypeOf(FileNotFoundError("")):    {},
	reflect.TypeOf(InvalidArgumentError("")): {},
	reflect.TypeOf(HasConflictsError{}):      {},
	reflect.TypeOf(ConflictingFilesError{}):  {},
	reflect.TypeOf(EmptyError{}):             {},
	reflect.TypeOf(IndexError("")):           {},
}

// Result is the serialized result.
type Result struct {
	// CommitID is the result of the call.
	CommitID string
	// Error is set if the call errord.
	//
	// Deprecated: Use Err instead. Error clashes with other serialized types
	// where Error is not of type error
	Error error
	// Err is set if an error occurred. Err must exist on all gob serialized
	// results so that all errors can be returned.
	Err error
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

// ConflictingFilesError is an error raised when there are conflicting files.
type ConflictingFilesError struct {
	// ConflictingFiles is the set of files which have conflicts.
	ConflictingFiles []string
}

func (err ConflictingFilesError) Error() string {
	return "there are conflicting files"
}

// EmptyError indicates the command, for example cherry-pick, did result in no
// changes, so the result is empty.
type EmptyError struct{}

func (err EmptyError) Error() string {
	return "could not apply because the result was empty"
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

	if _, ok := registeredTypes[reflect.TypeOf(err)]; !ok {
		return wrapError{Message: err.Error()}
	}

	return err
}
