package git

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReferenceName_NewReferenceNameFromBranchName(t *testing.T) {
	for _, tc := range []struct {
		desc      string
		reference string
		expected  string
	}{
		{
			desc:      "unqualified reference",
			reference: "master",
			expected:  "refs/heads/master",
		},
		{
			desc:      "partly qualified reference",
			reference: "heads/master",
			expected:  "refs/heads/heads/master",
		},
		{
			desc:      "fully qualified reference",
			reference: "refs/heads/master",
			expected:  "refs/heads/refs/heads/master",
		},
		{
			desc:      "weird branch name",
			reference: "refs/master",
			expected:  "refs/heads/refs/master",
		},
		{
			desc:      "tag is treated as a branch",
			reference: "refs/tags/master",
			expected:  "refs/heads/refs/tags/master",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ref := NewReferenceNameFromBranchName(tc.reference)
			require.Equal(t, ref.String(), tc.expected)
		})
	}
}

func TestReferenceName_Branch(t *testing.T) {
	for _, tc := range []struct {
		desc      string
		reference string
		expected  string
	}{
		{
			desc:      "fully qualified reference",
			reference: "refs/heads/master",
			expected:  "master",
		},
		{
			desc:      "nested branch",
			reference: "refs/heads/foo/master",
			expected:  "foo/master",
		},
		{
			desc:      "unqualified branch is not a branch",
			reference: "master",
			expected:  "",
		},
		{
			desc:      "tag is not a branch",
			reference: "refs/tags/master",
			expected:  "",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			branch, ok := ReferenceName(tc.reference).Branch()
			require.Equal(t, tc.expected, branch)
			require.Equal(t, tc.expected != "", ok)
		})
	}
}
