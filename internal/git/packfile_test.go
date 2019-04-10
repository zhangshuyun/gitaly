package git

import (
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

type packExample struct {
	desc    string
	n       uint32
	raw     []byte
	content []byte
}

var packExamples = []packExample{
	{
		desc:    "small, 3 objects",
		n:       3,
		raw:     []byte("PACK\x00\x00\x00\x02\x00\x00\x00\x03hello=?\x1A$\xB3\x8F\xCC\x96\xE0\xB0\xAC\xF0\x93\t\x85\xD8\x87K\xC5p"),
		content: []byte("hello"),
	},
}

func TestPackReader(t *testing.T) {
	for _, tc := range packExamples {
		t.Run(tc.desc, func(t *testing.T) {
			pr, err := NewPackReader(bytes.NewReader(tc.raw))
			require.NoError(t, err)

			require.Equal(t, tc.n, pr.NumObjects(), "number of objects in packfile")

			out, err := ioutil.ReadAll(pr)
			require.NoError(t, err, "read all data")

			require.Equal(t, string(tc.content), string(out), "packfile content")
		})
	}
}

// TODO add more PackReader tests: invalid header, length < 32, invalid checksum

func TestPackWriter(t *testing.T) {
	for _, tc := range packExamples {
		t.Run(tc.desc, func(t *testing.T) {
			out := &bytes.Buffer{}
			pw, err := NewPackWriter(out, tc.n)
			require.NoError(t, err)

			in := tc.content
			nBytes, err := pw.Write(in)
			require.NoError(t, err)
			require.Equal(t, nBytes, len(in), "bytes written")

			require.NoError(t, pw.Flush(), "flush")
			require.Equal(t, string(tc.raw), out.String())
		})
	}
}

func TestPackWriterFlush(t *testing.T) {
	out := &bytes.Buffer{}
	pw, err := NewPackWriter(out, 123)
	require.NoError(t, err)

	require.NoError(t, pw.Flush())

	n, err := pw.Write([]byte("hello"))
	require.Equal(t, 0, n, "bytes written should be 0")
	require.IsType(t, alreadyFlushedError{}, err, "write error should be 'already flushed'")

	require.IsType(t, alreadyFlushedError{}, pw.Flush(), "flush error should be 'already flushed'")
}
