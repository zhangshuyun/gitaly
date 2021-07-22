package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func TestValidateRelativePath(t *testing.T) {
	for _, tc := range []struct {
		path    string
		cleaned string
		error   error
	}{
		{"/parent", "parent", nil},
		{"parent/", "parent", nil},
		{"/parent-with-suffix", "parent-with-suffix", nil},
		{"/subfolder", "subfolder", nil},
		{"subfolder", "subfolder", nil},
		{"subfolder/", "subfolder", nil},
		{"subfolder//", "subfolder", nil},
		{"subfolder/..", ".", nil},
		{"subfolder/../..", "", ErrRelativePathEscapesRoot},
		{"/..", "", ErrRelativePathEscapesRoot},
		{"..", "", ErrRelativePathEscapesRoot},
		{"../", "", ErrRelativePathEscapesRoot},
		{"", ".", nil},
		{".", ".", nil},
	} {
		const parent = "/parent"
		t.Run(parent+" and "+tc.path, func(t *testing.T) {
			cleaned, err := ValidateRelativePath(parent, tc.path)
			assert.Equal(t, tc.cleaned, cleaned)
			assert.Equal(t, tc.error, err)
		})
	}
}

func TestQuarantineDirectoryPrefix(t *testing.T) {
	// An nil repository works alright, even if nonsensical.
	require.Equal(t, "quarantine-0000000000000000-", QuarantineDirectoryPrefix(nil))

	// A repository with only a relative path.
	require.Equal(t, "quarantine-8843d7f92416211d-", QuarantineDirectoryPrefix(&gitalypb.Repository{
		RelativePath: "foobar",
	}))

	// A different relative path has a different hash.
	require.Equal(t, "quarantine-60518c1c11dc0452-", QuarantineDirectoryPrefix(&gitalypb.Repository{
		RelativePath: "barfoo",
	}))

	// Only the relative path matters. The storage name doesn't matter either given that the
	// temporary directory is per storage.
	require.Equal(t, "quarantine-60518c1c11dc0452-", QuarantineDirectoryPrefix(&gitalypb.Repository{
		StorageName:        "storage-name",
		RelativePath:       "barfoo",
		GitObjectDirectory: "object-directory",
		GitAlternateObjectDirectories: []string{
			"alternate",
		},
		GlRepository:  "gl-repo",
		GlProjectPath: "gl/repo",
	}))
}
