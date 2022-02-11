package housekeeping

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
)

func TestIsPoolPath(t *testing.T) {
	for _, tc := range []struct {
		desc         string
		relativePath string
		isPoolPath   bool
	}{
		{
			desc:         "rails pool directory",
			relativePath: gittest.NewObjectPoolName(t),
			isPoolPath:   true,
		},
		{
			desc: "empty string",
		},
		{
			desc:         "rails path first to subdirs dont match full hash",
			relativePath: "@pools/aa/bb/ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff.git",
		},
		{
			desc:         "normal repos dont match",
			relativePath: "@hashed/" + gittest.NewRepositoryName(t, true),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.isPoolPath, IsPoolPath(tc.relativePath))
		})
	}
}
