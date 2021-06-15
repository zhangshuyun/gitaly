package git

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
)

var (
	// minimumVersion is the minimum required Git version. If updating this version, be sure to
	// also update the following locations:
	// - https://gitlab.com/gitlab-org/gitaly/blob/master/README.md#installation
	// - https://gitlab.com/gitlab-org/gitaly/blob/master/.gitlab-ci.yml
	// - https://gitlab.com/gitlab-org/gitlab-foss/blob/master/.gitlab-ci.yml
	// - https://gitlab.com/gitlab-org/gitlab-foss/blob/master/lib/system_check/app/git_version_check.rb
	minimumVersion = Version{
		versionString: "2.31.0",
		major:         2,
		minor:         31,
		patch:         0,
		rc:            false,
	}
)

// Version represents the version of git itself.
type Version struct {
	// versionString is the string representation of the version. The string representation is
	// not directly derived from the parsed version information because we do not extract all
	// information from the original version string. Deriving it from parsed information would
	// thus potentially lose information.
	versionString       string
	major, minor, patch uint32
	rc                  bool
}

// CurrentVersion returns the used git version.
func CurrentVersion(ctx context.Context, gitCmdFactory CommandFactory) (Version, error) {
	var buf bytes.Buffer
	cmd, err := gitCmdFactory.NewWithoutRepo(ctx, SubCmd{
		Name: "version",
	}, WithStdout(&buf))
	if err != nil {
		return Version{}, err
	}

	if err = cmd.Wait(); err != nil {
		return Version{}, err
	}

	out := strings.Trim(buf.String(), " \n")
	versionString := strings.SplitN(out, " ", 3)
	if len(versionString) != 3 {
		return Version{}, fmt.Errorf("invalid version format: %q", buf.String())
	}

	version, err := parseVersion(versionString[2])
	if err != nil {
		return Version{}, fmt.Errorf("cannot parse git version: %w", err)
	}

	return version, nil
}

// String returns the string representation of the version.
func (v Version) String() string {
	return v.versionString
}

// IsSupported checks if a version string corresponds to a Git version
// supported by Gitaly.
func (v Version) IsSupported() bool {
	return !v.LessThan(minimumVersion)
}

// SupportsObjectTypeFilter checks if a version corresponds to a Git version which supports object
// type filters.
func (v Version) SupportsObjectTypeFilter() bool {
	return !v.LessThan(Version{major: 2, minor: 32, patch: 0})
}

// LessThan determines whether the version is older than another version.
func (v Version) LessThan(other Version) bool {
	switch {
	case v.major < other.major:
		return true
	case v.major > other.major:
		return false

	case v.minor < other.minor:
		return true
	case v.minor > other.minor:
		return false

	case v.patch < other.patch:
		return true
	case v.patch > other.patch:
		return false

	case v.rc && !other.rc:
		return true
	case !v.rc && other.rc:
		return false

	default:
		// this should only be reachable when versions are equal
		return false
	}
}

func parseVersion(versionStr string) (Version, error) {
	versionSplit := strings.SplitN(versionStr, ".", 4)
	if len(versionSplit) < 3 {
		return Version{}, fmt.Errorf("expected major.minor.patch in %q", versionStr)
	}

	ver := Version{
		versionString: versionStr,
	}

	for i, v := range []*uint32{&ver.major, &ver.minor, &ver.patch} {
		var n64 uint64

		if versionSplit[i] == "GIT" {
			// Git falls back to vx.x.GIT if it's unable to describe the current version
			// or if there's a version file. We should just treat this as "0", even
			// though it may have additional commits on top.
			n64 = 0
		} else {
			rcSplit := strings.SplitN(versionSplit[i], "-", 2)

			var err error
			n64, err = strconv.ParseUint(rcSplit[0], 10, 32)
			if err != nil {
				return Version{}, err
			}

			if len(rcSplit) == 2 && strings.HasPrefix(rcSplit[1], "rc") {
				ver.rc = true
			}
		}

		*v = uint32(n64)
	}

	if len(versionSplit) == 4 {
		if strings.HasPrefix(versionSplit[3], "rc") {
			ver.rc = true
		}
	}

	return ver, nil
}
