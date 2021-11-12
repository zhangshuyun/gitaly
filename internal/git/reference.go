package git

import (
	"bytes"
	"context"
	"errors"
	"strings"
)

// InternalRefPrefixes is an array of all reference prefixes which are used internally by GitLab.
// These need special treatment in some cases, e.g. to restrict writing to them.
var InternalRefPrefixes = [...]string{
	"refs/environments/",
	"refs/keep-around/",
	"refs/merge-requests/",
	"refs/pipelines/",
}

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

// NewReferenceNameFromBranchName returns a new ReferenceName from a given
// branch name. Note that branch is treated as an unqualified branch name.
// This function will thus always prepend "refs/heads/".
func NewReferenceNameFromBranchName(branch string) ReferenceName {
	return ReferenceName("refs/heads/" + branch)
}

// String returns the string representation of the ReferenceName.
func (r ReferenceName) String() string {
	return string(r)
}

// Revision converts the ReferenceName to a Revision. This is safe to do as a
// reference is always also a revision.
func (r ReferenceName) Revision() Revision {
	return Revision(r)
}

// Branch returns `true` and the branch name if the reference is a branch. E.g.
// if ReferenceName is "refs/heads/master", it will return "master". If it is
// not a branch, `false` is returned.
func (r ReferenceName) Branch() (string, bool) {
	if strings.HasPrefix(r.String(), "refs/heads/") {
		return r.String()[len("refs/heads/"):], true
	}
	return "", false
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
func CheckRefFormat(ctx context.Context, gitCmdFactory CommandFactory, refName string) (bool, error) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	cmd, err := gitCmdFactory.NewWithoutRepo(ctx,
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

// GuessHead tries to guess what branch HEAD would be pointing at given a list
// of references. The references are assumed to be ordered by name.
//
// This function should match the corresponding function in git:
// https://github.com/git/git/blob/2a97289ad8b103625d3a1a12f66c27f50df822ce/remote.c#L2198
func GuessHead(refs []Reference) (ReferenceName, error) {
	head := findRefByName(refs, "HEAD")
	if head == nil {
		return "", errors.New("missing HEAD ref")
	}
	if head.IsSymbolic {
		return ReferenceName(head.Target), nil
	}

	main := findRefByName(refs, ReferenceName(DefaultRef))
	if main != nil && head.Target == main.Target {
		return main.Name, nil
	}

	master := findRefByName(refs, ReferenceName(LegacyDefaultRef))
	if master != nil && head.Target == master.Target {
		return master.Name, nil
	}

	for i := range refs {
		if _, ok := refs[i].Name.Branch(); !ok {
			continue
		}
		if refs[i].Target == head.Target {
			return refs[i].Name, nil
		}
	}

	return "", errors.New("no matching ref")
}

// findRefByName filters through refs and returns the first ref where name
// matches exactly. nil is returned when no match is found.
func findRefByName(refs []Reference, name ReferenceName) *Reference {
	for i := range refs {
		if refs[i].Name == name {
			return &refs[i]
		}
	}
	return nil
}
