package catfile

import (
	"bufio"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseObjectInfoSuccess(t *testing.T) {
	testCases := []struct {
		desc     string
		input    string
		output   *ObjectInfo
		notFound bool
	}{
		{
			desc:  "existing object",
			input: "7c9373883988204e5a9f72c4a5119cbcefc83627 commit 222\n",
			output: &ObjectInfo{
				Oid:  "7c9373883988204e5a9f72c4a5119cbcefc83627",
				Type: "commit",
				Size: 222,
			},
		},
		{
			desc:     "non existing object",
			input:    "bla missing\n",
			notFound: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(tc.input))
			output, err := ParseObjectInfo(reader)
			if tc.notFound {
				require.True(t, IsNotFound(err), "expect NotFoundError")
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.output, output)
		})
	}
}

func TestParseObjectInfoErrors(t *testing.T) {
	testCases := []struct {
		desc  string
		input string
	}{
		{desc: "missing newline", input: "7c9373883988204e5a9f72c4a5119cbcefc83627 commit 222"},
		{desc: "too few words", input: "7c9373883988204e5a9f72c4a5119cbcefc83627 commit\n"},
		{desc: "too many words", input: "7c9373883988204e5a9f72c4a5119cbcefc83627 commit 222 bla\n"},
		{desc: "parse object size", input: "7c9373883988204e5a9f72c4a5119cbcefc83627 commit bla\n"},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(tc.input))
			_, err := ParseObjectInfo(reader)

			require.Error(t, err)
		})
	}
}
