package praefectutil

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
)

func TestDeriveReplicaPath(t *testing.T) {
	require.Equal(t, "@cluster/repositories/6b/86/1", DeriveReplicaPath(1))
	require.Equal(t, "@cluster/repositories/d4/73/2", DeriveReplicaPath(2))
}

func TestDerivePoolPath(t *testing.T) {
	require.Equal(t, "@cluster/pools/6b/86/1", DerivePoolPath(1))
	require.Equal(t, "@cluster/pools/d4/73/2", DerivePoolPath(2))
}

func TestIsPoolPath(t *testing.T) {
	for _, tc := range []struct {
		desc         string
		relativePath string
		isPoolPath   bool
	}{
		{
			desc:         "praefect pool path",
			relativePath: DerivePoolPath(1),
			isPoolPath:   true,
		},
		{
			desc:         "praefect replica path",
			relativePath: DeriveReplicaPath(1),
		},
		{
			desc:         "rails pool path",
			relativePath: gittest.NewObjectPoolName(t),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.isPoolPath, IsPoolPath(tc.relativePath))
		})
	}
}
