package lines

import (
	"bytes"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLinesSend(t *testing.T) {
	expected := [][]byte{
		[]byte("mepmep"),
		[]byte("foo"),
		[]byte("bar"),
	}

	tcs := []struct {
		desc           string
		filter         *regexp.Regexp
		limit          int
		isPageToken    func([]byte) bool
		PageTokenError bool
		output         [][]byte
	}{
		{
			desc:   "high limit",
			limit:  100,
			output: expected,
		},
		{
			desc:   "limit is 0",
			limit:  0,
			output: [][]byte(nil),
		},
		{
			desc:   "limit 2",
			limit:  2,
			output: expected[0:2],
		},
		{
			desc:   "filter and limit",
			limit:  1,
			filter: regexp.MustCompile("foo"),
			output: expected[1:2],
		},
		{
			desc:        "skip lines",
			limit:       100,
			isPageToken: func(line []byte) bool { return bytes.HasPrefix(line, expected[0]) },
			output:      expected[1:3],
		},
		{
			desc:        "page token is invalid",
			limit:       100,
			isPageToken: func(line []byte) bool { return false },
			output:      [][]byte(nil),
		},
		{
			desc:           "page token is invalid and page token error is set",
			limit:          100,
			isPageToken:    func(line []byte) bool { return false },
			PageTokenError: true,
		},
		{
			desc:        "skip no lines",
			limit:       100,
			isPageToken: func(_ []byte) bool { return true },
			output:      expected,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.desc, func(t *testing.T) {
			reader := bytes.NewBufferString("mepmep\nfoo\nbar")
			var out [][]byte
			sender := func(in [][]byte) error { out = in; return nil }

			err := Send(reader, sender, SenderOpts{
				Limit:          tc.limit,
				IsPageToken:    tc.isPageToken,
				PageTokenError: tc.PageTokenError,
				Filter:         tc.filter,
				Delimiter:      '\n',
			})

			if tc.PageTokenError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.output, out)
			}
		})
	}
}
