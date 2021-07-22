package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
