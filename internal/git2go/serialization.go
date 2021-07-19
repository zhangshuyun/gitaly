package git2go

import (
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
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
	EmptyError{}:             {},
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

	if _, ok := registeredTypes[reflect.Zero(reflect.TypeOf(err)).Interface()]; !ok {
		return wrapError{Message: err.Error()}
	}

	return err
}

func serialize(v interface{}) (string, error) {
	marshalled, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(marshalled), nil
}

func deserialize(serialized string, v interface{}) error {
	base64Decoder := base64.NewDecoder(base64.StdEncoding, strings.NewReader(serialized))
	jsonDecoder := json.NewDecoder(base64Decoder)
	return jsonDecoder.Decode(v)
}

func serializeTo(writer io.Writer, v interface{}) error {
	base64Encoder := base64.NewEncoder(base64.StdEncoding, writer)
	defer base64Encoder.Close()
	jsonEncoder := json.NewEncoder(base64Encoder)
	return jsonEncoder.Encode(v)
}
