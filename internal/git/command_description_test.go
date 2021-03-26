package git

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommandDescriptions_mayGeneratePackfiles(t *testing.T) {
	gcDescription, ok := commandDescriptions["gc"]
	require.True(t, ok)
	require.True(t, gcDescription.mayGeneratePackfiles())

	applyDescription, ok := commandDescriptions["apply"]
	require.True(t, ok)
	require.False(t, applyDescription.mayGeneratePackfiles())
}
