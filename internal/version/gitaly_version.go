package version

import (
	"fmt"
)

var version string
var buildtime string
var moduleVersion string

// GetVersionString returns a standard version header
func GetVersionString() string {
	return fmt.Sprintf("Gitaly, version %v", version)
}

// GetVersion returns the semver compatible version number
func GetVersion() string {
	return version
}

// GetBuildTime returns the time at which the build took place
func GetBuildTime() string {
	return buildtime
}

// GetModuleVersion returns the version of the module, like v13 or v20.
func GetModuleVersion() string {
	return moduleVersion
}
