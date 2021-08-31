package git2go

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConflictsCommand_Serialization(t *testing.T) {
	testcases := []struct {
		desc string
		cmd  ConflictsCommand
		err  string
	}{
		{
			desc: "missing repository",
			cmd:  ConflictsCommand{},
			err:  "missing repository",
		},
		{
			desc: "missing theirs",
			cmd: ConflictsCommand{
				Repository: "foo",
				Ours:       "refs/heads/master",
			},
			err: "missing theirs",
		},
		{
			desc: "missing ours",
			cmd: ConflictsCommand{
				Repository: "foo",
				Theirs:     "refs/heads/master",
			},
			err: "missing ours",
		},
		{
			desc: "valid command",
			cmd: ConflictsCommand{
				Repository: "foo",
				Ours:       "refs/heads/master",
				Theirs:     "refs/heads/foo",
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			serialized, err := serialize(tc.cmd)
			require.NoError(t, err)

			deserialized, err := ConflictsCommandFromSerialized(serialized)

			if tc.err != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.err)
			} else {
				require.NoError(t, err)
				require.Equal(t, deserialized, tc.cmd)
			}
		})
	}
}

func TestConflictsResult_Serialization(t *testing.T) {
	serializeResult := func(t *testing.T, result ConflictsResult) string {
		var buf bytes.Buffer
		err := result.SerializeTo(&buf)
		require.NoError(t, err)
		return buf.String()
	}

	testcases := []struct {
		desc       string
		serialized string
		expected   ConflictsResult
		err        string
	}{
		{
			desc:       "empty merge result",
			serialized: serializeResult(t, ConflictsResult{}),
			expected:   ConflictsResult{},
		},
		{
			desc: "merge result with commit",
			serialized: serializeResult(t, ConflictsResult{
				Conflicts: []Conflict{
					{
						Ancestor: ConflictEntry{Path: "dir/ancestor", Mode: 0o100644},
						Our:      ConflictEntry{Path: "dir/our", Mode: 0o100644},
						Their:    ConflictEntry{Path: "dir/their", Mode: 0o100644},
						Content:  []byte("content"),
					},
				},
			}),
			expected: ConflictsResult{
				Conflicts: []Conflict{
					{
						Ancestor: ConflictEntry{Path: "dir/ancestor", Mode: 0o100644},
						Our:      ConflictEntry{Path: "dir/our", Mode: 0o100644},
						Their:    ConflictEntry{Path: "dir/their", Mode: 0o100644},
						Content:  []byte("content"),
					},
				},
			},
		},
		{
			desc:       "invalid serialized representation",
			serialized: "xvlc",
			err:        "invalid character",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			var deserialized ConflictsResult
			err := deserialize(tc.serialized, &deserialized)

			if tc.err != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.err)
			} else {
				require.NoError(t, err)
				require.Equal(t, deserialized, tc.expected)
			}
		})
	}
}
