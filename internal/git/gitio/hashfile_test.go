package gitio

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHashfileReader(t *testing.T) {
	testCases := []struct {
		desc string
		in   string
		out  string
		fail bool
	}{
		{
			desc: "simple input",
			in:   "hello\xaa\xf4\xc6\x1d\xdc\xc5\xe8\xa2\xda\xbe\xde\x0f\x3b\x48\x2c\xd9\xae\xa9\x43\x4d",
			out:  "hello",
		},
		{
			desc: "empty input",
			in:   "\xda\x39\xa3\xee\x5e\x6b\x4b\x0d\x32\x55\xbf\xef\x95\x60\x18\x90\xaf\xd8\x07\x09",
			out:  "",
		},
		{
			desc: "checksum mismatch",
			in:   "hello\xff\xf4\xc6\x1d\xdc\xc5\xe8\xa2\xda\xbe\xde\x0f\x3b\x48\x2c\xd9\xae\xa9\x43\x4d",
			out:  "hello",
			fail: true,
		},
		{
			desc: "input too short",
			in:   "hello world",
			out:  "",
			fail: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			r := NewHashfileReader(strings.NewReader(tc.in))
			out, err := io.ReadAll(r)
			if tc.fail {
				require.Error(t, err, "invalid input should cause error")
				return
			}

			require.NoError(t, err, "valid input")
			require.Equal(t, tc.out, string(out), "compare output")
		})
	}
}
