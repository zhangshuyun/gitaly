package git

import (
	"bytes"
	"context"
)

// Revision represents anything that resolves to either a commit, multiple
// commits or to an object different than a commit. This could be e.g.
// "master", "master^{commit}", an object hash or similar. See gitrevisions(1)
// for supported syntax.
type Revision string

// String returns the string representation of the Revision.
func (r Revision) String() string {
	return string(r)
}

// ReferenceName represents the name of a git reference, e.g.
// "refs/heads/master". It does not support extended revision notation like a
// Revision does and must always contain a fully qualified reference.
type ReferenceName string

// String returns the string representation of the ReferenceName.
func (r ReferenceName) String() string {
	return string(r)
}

// Revision converts the ReferenceName to a Revision. This is safe to do as a
// reference is always also a revision.
func (r ReferenceName) Revision() Revision {
	return Revision(r)
}

// Reference represents a Git reference.
type Reference struct {
	// Name is the name of the reference
	Name ReferenceName
	// Target is the target of the reference. For direct references it
	// contains the object ID, for symbolic references it contains the
	// target branch name.
	Target string
	// IsSymbolic tells whether the reference is direct or symbolic
	IsSymbolic bool
}

// NewReference creates a direct reference to an object.
func NewReference(name ReferenceName, target string) Reference {
	return Reference{
		Name:       name,
		Target:     target,
		IsSymbolic: false,
	}
}

// NewSymbolicReference creates a symbolic reference to another reference.
func NewSymbolicReference(name ReferenceName, target string) Reference {
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
