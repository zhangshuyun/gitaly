package praefectutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeriveReplicaPath(t *testing.T) {
	require.Equal(t, "@praefect/v1/6b/86/1", DeriveReplicaPath(1))
	require.Equal(t, "@praefect/v1/d4/73/2", DeriveReplicaPath(2))
}
