package git

import (
	"bytes"
	"regexp"
)

var (
	lfsOIDRe  = regexp.MustCompile(`(?m)^oid sha256:[0-9a-f]{64}$`)
	lfsSizeRe = regexp.MustCompile(`(?m)^size [0-9]+$`)
)

// IsLFSPointer checks to see if a blob is an LFS pointer. It returns the raw data of the pointer if it is
func IsLFSPointer(b []byte) bool {
	// ensure the version exists
	if !bytes.HasPrefix(b, []byte("version https://git-lfs.github.com/spec")) {
		return false
	}

	// ensure the oid exists
	if !lfsOIDRe.Match(b) {
		return false
	}

	// ensure the size exists
	if !lfsSizeRe.Match(b) {
		return false
	}

	return true
}
