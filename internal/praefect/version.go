package praefect

import (
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v14/internal/version"
)

// GetVersionString returns a standard version header
func GetVersionString() string {
	return fmt.Sprintf("Praefect, version %v", version.GetVersion())
}

// GetVersion returns the semver compatible version number
func GetVersion() string {
	return version.GetVersion()
}

// GetBuildTime returns the time at which the build took place
func GetBuildTime() string {
	return version.GetBuildTime()
}
