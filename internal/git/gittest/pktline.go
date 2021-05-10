package gittest

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/pktline"
)

// WritePktlineString writes the pktline-formatted data into the writer.
func WritePktlineString(t testing.TB, writer io.Writer, data string) {
	_, err := pktline.WriteString(writer, data)
	require.NoError(t, err)
}

// WritePktlineFlush writes the pktline-formatted flush into the writer.
func WritePktlineFlush(t testing.TB, writer io.Writer) {
	require.NoError(t, pktline.WriteFlush(writer))
}

// WritePktlineDone writes the pktline-formatted Done into the writer.
func WritePktlineDone(t testing.TB, writer io.Writer) {
	require.NoError(t, pktline.WriteDone(writer))
}

// WritePktlineDelim writes the pktline-formatted delimiter into the writer.
func WritePktlineDelim(t *testing.T, writer io.Writer) {
	require.NoError(t, pktline.WriteDelim(writer))
}
