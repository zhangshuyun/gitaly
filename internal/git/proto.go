package git

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
)

// FallbackTimeValue is the value returned by `SafeTimeParse` in case it
// encounters a parse error. It's the maximum time value possible in golang.
// See https://gitlab.com/gitlab-org/gitaly/issues/556#note_40289573
var FallbackTimeValue = time.Unix(1<<63-62135596801, 999999999)

func validateRevision(revision []byte, allowEmpty bool) error {
	if !allowEmpty && len(revision) == 0 {
		return fmt.Errorf("empty revision")
	}
	if bytes.HasPrefix(revision, []byte("-")) {
		return fmt.Errorf("revision can't start with '-'")
	}
	if bytes.Contains(revision, []byte(" ")) {
		return fmt.Errorf("revision can't contain whitespace")
	}
	if bytes.Contains(revision, []byte("\x00")) {
		return fmt.Errorf("revision can't contain NUL")
	}
	if bytes.Contains(revision, []byte(":")) {
		return fmt.Errorf("revision can't contain ':'")
	}
	return nil
}

// ValidateRevisionAllowEmpty checks if a revision looks valid, but allows
// empty strings
func ValidateRevisionAllowEmpty(revision []byte) error {
	return validateRevision(revision, true)
}

// ValidateRevision checks if a revision looks valid
func ValidateRevision(revision []byte) error {
	return validateRevision(revision, false)
}

// Version returns the used git version.
func Version() (string, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var buf bytes.Buffer
	cmd, err := BareCommand(ctx, nil, &buf, nil, nil, "version")
	if err != nil {
		return "", err
	}

	if err = cmd.Wait(); err != nil {
		return "", err
	}

	out := strings.Trim(buf.String(), " \n")
	ver := strings.SplitN(out, " ", 3)
	if len(ver) != 3 {
		return "", fmt.Errorf("invalid version format: %q", buf.String())
	}

	return ver[2], nil
}

// VersionLessThan returns true if the parsed version value of v1Str is less
// than the parsed version value of v2Str. An error can be returned if the
// strings cannot be parsed.
// Note: this is an extremely simplified semver comparison algorithm
func VersionLessThan(v1Str, v2Str string) (bool, error) {
	var (
		v1, v2 version
		err    error
	)

	for _, v := range []struct {
		string
		*version
	}{
		{v1Str, &v1},
		{v2Str, &v2},
	} {
		*v.version, err = parseVersion(v.string)
		if err != nil {
			return false, err
		}
	}

	return versionLessThan(v1, v2), nil
}

func versionLessThan(v1, v2 version) bool {
	switch {

	case v1.major < v2.major:
		return true
	case v1.major > v2.major:
		return false

	case v1.minor < v2.minor:
		return true
	case v1.minor > v2.minor:
		return false

	case v1.patch < v2.patch:
		return true
	case v1.patch > v2.patch:
		return false

	default:
		// this should only be reachable when versions are equal
		return false

	}
}

type version struct {
	major, minor, patch uint32
}

func parseVersion(versionStr string) (version, error) {
	versionSplit := strings.SplitN(versionStr, ".", 3)
	if len(versionSplit) < 3 {
		return version{}, fmt.Errorf("expected major.minor.patch in %q", versionStr)
	}

	var ver version

	for i, v := range []*uint32{&ver.major, &ver.minor, &ver.patch} {
		n64, err := strconv.ParseUint(versionSplit[i], 10, 32)
		if err != nil {
			return version{}, err
		}

		*v = uint32(n64)
	}

	return ver, nil
}

// SupportsDeltaIslands checks if a version string (e.g. "2.20.0")
// corresponds to a Git version that supports delta islands.
func SupportsDeltaIslands(versionStr string) (bool, error) {
	v, err := parseVersion(versionStr)
	if err != nil {
		return false, err
	}

	return !versionLessThan(v, version{2, 20, 0}), nil
}

// NoMissingWantErrMessage checks if the git version is before Git 2.22,
// in which versions the missing objects in the wants didn't yield an explicit
// error message, but no ouput at all.
func NoMissingWantErrMessage() bool {
	ver, err := Version()
	if err != nil {
		return false
	}

	lt, err := VersionLessThan(ver, "2.22.0")
	return err == nil && lt
}

// BuildGitOptions helps to generate options to the git command.
// If gitOpts is not empty then its values are passed as part of
// the "-c" option of the git command, the other values are passed along with the subcommand.
func BuildGitOptions(gitOpts []string, otherOpts ...string) []string {
	args := []string{}

	for _, opt := range gitOpts {
		args = append(args, "-c", opt)
	}

	return append(args, otherOpts...)
}

// InfoAlternatesPath finds the fully qualified path for the alternates file.
func InfoAlternatesPath(repo repository.GitRepo) (string, error) {
	repoPath, err := helper.GetRepoPath(repo)
	if err != nil {
		return "", err
	}

	return filepath.Join(repoPath, "objects", "info", "alternates"), nil
}
