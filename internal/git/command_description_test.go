package git

import (
	"fmt"
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

func TestCommandDescriptions_revListPositionalArgs(t *testing.T) {
	revlist, ok := commandDescriptions["rev-list"]
	require.True(t, ok)
	require.NotNil(t, revlist.validatePositionalArgs)

	for _, tc := range []struct {
		desc        string
		args        []string
		expectedErr error
	}{
		{
			desc: "normal reference",
			args: []string{
				"master",
			},
		},
		{
			desc: "reference with leading dash",
			args: []string{
				"-master",
			},
			expectedErr: fmt.Errorf("rev-list: %w",
				fmt.Errorf("positional arg \"-master\" cannot start with dash '-': %w", ErrInvalidArg),
			),
		},
		{
			desc: "revisions and pseudo-revisions",
			args: []string{
				"master --not --all",
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			err := revlist.validatePositionalArgs(tc.args)
			require.Equal(t, tc.expectedErr, err)
		})
	}
}
