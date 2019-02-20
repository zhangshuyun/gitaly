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
		assert.Equal(t, tc.containsTraversal, ContainsPathTraversal(tc.path))
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

func TestValidSha(t *testing.T) {
	testCases := []struct {
		sha   string
		error bool
	}{
		{"", true},
		{"invalid-sha", true},
		{"878d0d962673697c1d038d47e8070f8e7a807028", false},
		{"878d0d962673697c1d038d47e8070f8e7a80702", true},
		{"878d0d962673697c1d038d47e8070f8e7a807028a", true},
	}

	for _, tc := range testCases {
		if tc.error {
			assert.Error(t, ValidSha(tc.sha), "Invalid Sha")
		} else {
			assert.NoError(t, ValidSha(tc.sha))
		}
	}
}
