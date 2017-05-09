package helper

import (
	"bytes"
)

// ByteSliceHasAnyPrefix tests whether the byte slice s begins with any of the prefixes.
func ByteSliceHasAnyPrefix(s []byte, prefixes ...string) bool {
	for _, prefix := range prefixes {
		if bytes.HasPrefix(s, []byte(prefix)) {
			return true
		}
	}

	return false
}

// IsNumber tests whether the byte slice s contains only digits or not
func IsNumber(s []byte) bool {
	for i := range s {
		if !bytes.Contains([]byte("1234567890"), s[i:i+1]) {
			return false
		}
	}
	return true
}

// UnquoteBytes removes surrounding double-quotes from a byte slice returning
// a new slice if they exist, otherwise it returns the same byte slice passed.
func UnquoteBytes(s []byte) []byte {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}

	return s
}
