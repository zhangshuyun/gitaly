package helper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestContainsPathTraversal(t *testing.T) {
	testCases := []struct {
		path              string
		containsTraversal bool
	}{
		{"../parent", true},
		{"subdir/../../parent", true},
		{"subdir/..", true},
		{"subdir", false},
		{"./subdir", false},
	}

	for _, tc := range testCases {
		assert.Equal(t, tc.containsTraversal, ContainsPathTraversal("/repo/storage/path", tc.path))
	}
}

func TestSanitizeString(t *testing.T) {
	testCases := []struct {
		input  string
		output string
	}{
		{"https://foo_the_user@gitlab.com/foo/bar", "https://[FILTERED]@gitlab.com/foo/bar"},
		{"https://foo_the_user:hUntEr1@gitlab.com/foo/bar", "https://[FILTERED]@gitlab.com/foo/bar"},
		{"proto://user:password@gitlab.com", "proto://[FILTERED]@gitlab.com"},
		{"some message proto://user:password@gitlab.com", "some message proto://[FILTERED]@gitlab.com"},
		{"test", "test"},
		{"ssh://@gitlab.com", "ssh://@gitlab.com"},
	}

	for _, tc := range testCases {
		assert.Equal(t, tc.output, SanitizeString(tc.input))
	}
}
