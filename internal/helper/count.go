package helper

import "io"

// CountingWriter wraps an io.Writer and counts all the writes. Accessing
// the count N is not thread-safe.
type CountingWriter struct {
	W io.Writer
	N int64
}

func (cw *CountingWriter) Write(p []byte) (int, error) {
	n, err := cw.W.Write(p)
	cw.N += int64(n)
	return n, err
}
