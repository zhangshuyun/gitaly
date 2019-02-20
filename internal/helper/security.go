package helper

import (
	"fmt"
	"os"
	"regexp"
	"strings"
)

// ContainsPathTraversal checks if the path contains any traversal
func ContainsPathTraversal(path string) bool {
	// Disallow directory traversal for security
	separator := string(os.PathSeparator)
	return strings.HasPrefix(path, ".."+separator) ||
		strings.Contains(path, separator+".."+separator) ||
		strings.HasSuffix(path, separator+"..")
}

// Pattern taken from Regular Expressions Cookbook, slightly modified though
//                                        |Scheme                |User                         |Named/IPv4 host|IPv6+ host
var hostPattern = regexp.MustCompile(`(?i)([a-z][a-z0-9+\-.]*://)([a-z0-9\-._~%!$&'()*+,;=:]+@)([a-z0-9\-._~%]+|\[[a-z0-9\-._~%!$&'()*+,;=:]+\])`)

// SanitizeString will clean password and tokens from URLs, and replace them
// with [FILTERED].
func SanitizeString(str string) string {
	return hostPattern.ReplaceAllString(str, "$1[FILTERED]@$3$4")
}

// ValidSha checks if SHA is valid
func ValidSha(sha string) error {
	if match, err := regexp.MatchString(`\A[0-9a-f]{40}\z`, sha); !match || err != nil {
		return fmt.Errorf("Invalid Sha")
	}

	return nil
}
