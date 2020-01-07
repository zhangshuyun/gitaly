package helper

import (
	"errors"
	"path"
	"regexp"
	"strings"
)

// ContainsPathTraversal checks if relPath contains any traversal
func ContainsPathTraversal(baseDir string, relPath string) bool {
	// Disallow directory traversal for security
	absPath := path.Clean((path.Join(baseDir, relPath)))
	return !strings.HasPrefix(absPath, baseDir)
}

// Pattern taken from Regular Expressions Cookbook, slightly modified though
//                                        |Scheme                |User                         |Named/IPv4 host|IPv6+ host
var hostPattern = regexp.MustCompile(`(?i)([a-z][a-z0-9+\-.]*://)([a-z0-9\-._~%!$&'()*+,;=:]+@)([a-z0-9\-._~%]+|\[[a-z0-9\-._~%!$&'()*+,;=:]+\])`)

// SanitizeString will clean password and tokens from URLs, and replace them
// with [FILTERED].
func SanitizeString(str string) string {
	return hostPattern.ReplaceAllString(str, "$1[FILTERED]@$3$4")
}

// SanitizeError does the same thing as SanitizeString but for error types
func SanitizeError(err error) error {
	return errors.New(SanitizeString(err.Error()))
}
