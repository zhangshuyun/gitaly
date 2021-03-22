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
	// - https://gitlab.com/gitlab-org/gitlab-build-images/blob/master/.gitlab-ci.yml
	// - https://gitlab.com/gitlab-org/gitlab-foss/blob/master/.gitlab-ci.yml
	// - https://gitlab.com/gitlab-org/gitlab-foss/blob/master/lib/system_check/app/git_version_check.rb
	// - https://gitlab.com/gitlab-org/build/CNG/blob/master/ci_files/variables.yml
	minimumVersion = version{2, 29, 0, false}
)

type version struct {
	major, minor, patch uint32
	rc                  bool
}

// Version returns the used git version.
func Version(ctx context.Context, gitCmdFactory CommandFactory) (string, error) {
	var buf bytes.Buffer
	cmd, err := gitCmdFactory.NewWithoutRepo(ctx, SubCmd{
		Name: "version",
	}, WithStdout(&buf))
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

	case v1.rc && !v2.rc:
		return true
	case !v1.rc && v2.rc:
		return false

	default:
		// this should only be reachable when versions are equal
		return false
	}
}

func parseVersion(versionStr string) (version, error) {
	versionSplit := strings.SplitN(versionStr, ".", 4)
	if len(versionSplit) < 3 {
		return version{}, fmt.Errorf("expected major.minor.patch in %q", versionStr)
	}

	var ver version

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
				return version{}, err
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

// SupportedVersion checks if a version string corresponds to a Git version
// supported by Gitaly.
func SupportedVersion(versionStr string) (bool, error) {
	v, err := parseVersion(versionStr)
	if err != nil {
		return false, err
	}

	return !versionLessThan(v, minimumVersion), nil
}
