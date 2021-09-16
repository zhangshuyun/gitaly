package gitio

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTrailerReaderSuccess(t *testing.T) {
	const trailerLen = 5

	testCases := []struct {
		desc    string
		in      string
		out     string
		trailer string
	}{
		{
			desc:    "large input",
			in:      strings.Repeat("hello", 4000) + "world",
			out:     strings.Repeat("hello", 4000),
			trailer: "world",
		},
		{
			desc:    "small input",
			in:      "hello world",
			out:     "hello ",
			trailer: "world",
		},
		{
			desc:    "smallest input",
			in:      "world",
			out:     "",
			trailer: "world",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			tr := NewTrailerReader(strings.NewReader(tc.in), trailerLen)
			require.Len(t, tc.trailer, trailerLen, "test case trailer sanity check")

			out, err := io.ReadAll(tr)
			require.NoError(t, err, "read all")
			require.Equal(t, tc.out, string(out), "compare output")

			trailer, err := tr.Trailer()
			require.NoError(t, err, "trailer error")
			require.Equal(t, tc.trailer, string(trailer), "compare trailer")
		})
	}
}

func TestTrailerReaderFail(t *testing.T) {
	const in = "hello world"
	const trailerLen = 100
	require.True(t, len(in) < trailerLen, "sanity check")

	tr := NewTrailerReader(strings.NewReader(in), trailerLen)

	_, err := tr.Trailer()
	require.Error(t, err, "Trailer() should fail when called too early")

	out, err := io.ReadAll(tr)
	require.NoError(t, err, "read")
	require.Empty(t, out, "read output")

	_, err = tr.Trailer()
	require.Error(t, err, "Trailer() should fail if there is not enough data")
}
