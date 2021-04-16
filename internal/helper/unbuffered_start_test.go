package helper

import (
	"bufio"
	"testing"

	"github.com/stretchr/testify/require"
)

type writeLogger struct {
	writes []string
}

func (wl *writeLogger) Write(p []byte) (int, error) {
	wl.writes = append(wl.writes, string(p))
	return len(p), nil
}

func TestUnbufferedStartWriter(t *testing.T) {
	wl := &writeLogger{}
	ssw := NewUnbufferedStartWriter(bufio.NewWriter(wl), 5)

	for _, c := range []byte("hello world") {
		n, err := ssw.Write([]byte{c})
		require.NoError(t, err)
		require.Equal(t, 1, n)
	}
	require.NoError(t, ssw.Flush())

	require.Equal(t, []string{"h", "e", "l", "l", "o", " world"}, wl.writes)
}
