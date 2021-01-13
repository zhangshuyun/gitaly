package git

import (
	"bytes"
	"context"
)

// Reference represents a Git reference.
type Reference struct {
	// Name is the name of the reference
	Name string
	// Target is the target of the reference. For direct references it
	// contains the object ID, for symbolic references it contains the
	// target branch name.
	Target string
	// IsSymbolic tells whether the reference is direct or symbolic
	IsSymbolic bool
}

// NewReference creates a direct reference to an object.
func NewReference(name, target string) Reference {
	return Reference{
		Name:       name,
		Target:     target,
		IsSymbolic: false,
	}
}

// NewSymbolicReference creates a symbolic reference to another reference.
func NewSymbolicReference(name, target string) Reference {
	return Reference{
		Name:       name,
		Target:     target,
		IsSymbolic: true,
	}
}

// CheckRefFormatError is used by CheckRefFormat() below
type CheckRefFormatError struct{}

func (e CheckRefFormatError) Error() string {
	return ""
}

// CheckRefFormat checks whether a fully-qualified refname is well
// well-formed using git-check-ref-format
func CheckRefFormat(ctx context.Context, refName string) (bool, error) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	cmd, err := NewCommandWithoutRepo(ctx, nil,
		SubCmd{
			Name: "check-ref-format",
			Args: []string{refName},
		},
		WithStdout(stdout),
		WithStderr(stderr),
	)
	if err != nil {
		return false, err
	}

	if err := cmd.Wait(); err != nil {
		return false, CheckRefFormatError{}
	}

	return true, nil
}
