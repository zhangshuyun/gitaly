package helper

import (
	"bufio"
)

// UnbufferedStartWriter wraps a *bufio.Writer so that early writes flush after each write.
type UnbufferedStartWriter struct {
	w         *bufio.Writer
	slowStart int64
	n         int64
}

// NewUnbufferedStartWriter returns a new UnbufferedStartWriter. Early writes
// automatically call Flush on the underlying writer. Once the total
// number of bytes written exceeds slowStart, the automatic flushing
// stops. Callers should always call Flush when they are done.
func NewUnbufferedStartWriter(w *bufio.Writer, slowStart int64) *UnbufferedStartWriter {
	return &UnbufferedStartWriter{w: w, slowStart: slowStart}
}

func (ssw *UnbufferedStartWriter) Write(p []byte) (int, error) {
	n, err := ssw.w.Write(p)
	ssw.n += int64(n)
	if err == nil && ssw.n <= ssw.slowStart {
		err = ssw.Flush()
	}
	return n, err
}

// Flush flushes the underlying bufio.Writer of ssw.
func (ssw *UnbufferedStartWriter) Flush() error { return ssw.w.Flush() }
