package git

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSubcommand_mayGeneratePackfiles(t *testing.T) {
	gcCmd, ok := gitCommands["gc"]
	require.True(t, ok)
	require.True(t, gcCmd.mayGeneratePackfiles())

	applyCmd, ok := gitCommands["apply"]
	require.True(t, ok)
	require.False(t, applyCmd.mayGeneratePackfiles())
}
