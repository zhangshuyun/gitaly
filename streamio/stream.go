// Package streamio contains wrappers intended for turning gRPC streams
// that send/receive messages with a []byte field into io.Writers and
// io.Readers.
//
package streamio

import (
	"errors"
	"io"
	"os"
	"strconv"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	methodCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gitaly_streamio_method_calls_total",
			Help: "Usage counters of gitaly streamio methods",
		}, []string{"method"},
	)
)

func init() {
	prometheus.MustRegister(methodCount)

	bufSize64, err := strconv.ParseInt(os.Getenv("GITALY_STREAMIO_WRITE_BUFFER_SIZE"), 10, 32)
	if err == nil && bufSize64 > 0 {
		WriteBufferSize = int(bufSize64)
	}
}

// NewReader turns receiver into an io.Reader. Errors from the receiver
// function are passed on unmodified. This means receiver should emit
// io.EOF when done.
func NewReader(receiver func() ([]byte, error)) io.Reader {
	return &receiveReader{receiver: receiver}
}

type receiveReader struct {
	receiver func() ([]byte, error)
	data     []byte
	err      error
}

func countMethod(method string) { methodCount.WithLabelValues(method).Inc() }

func (rr *receiveReader) Read(p []byte) (int, error) {
	countMethod("reader.Read")

	if len(rr.data) == 0 {
		rr.data, rr.err = rr.receiver()
	}

	n := copy(p, rr.data)
	rr.data = rr.data[n:]

	// We want to return any potential error only in case we have no
	// buffered data left. Otherwise, it can happen that we do not relay
	// bytes when the reader returns both data and an error.
	if len(rr.data) == 0 {
		return n, rr.err
	}

	return n, nil
}

// WriteTo implements io.WriterTo.
func (rr *receiveReader) WriteTo(w io.Writer) (int64, error) {
	countMethod("reader.WriteTo")

	var written int64

	// Deal with left-over state in rr.data and rr.err, if any
	if len(rr.data) > 0 {
		n, err := w.Write(rr.data)
		written += int64(n)
		if err != nil {
			return written, err
		}
	}
	if rr.err != nil {
		return written, rr.err
	}

	// Consume the response stream
	var errRead, errWrite error
	var n int
	var buf []byte
	for errWrite == nil && errRead != io.EOF {
		buf, errRead = rr.receiver()
		if errRead != nil && errRead != io.EOF {
			return written, errRead
		}

		if len(buf) > 0 {
			n, errWrite = w.Write(buf)
			written += int64(n)
		}
	}

	return written, errWrite
}

// NewWriter turns sender into an io.Writer. The sender callback will
// receive []byte arguments of length at most WriteBufferSize.
func NewWriter(sender func(p []byte) error) io.Writer {
	return &sendWriter{sender: sender}
}

// NewSyncWriter turns sender into an io.Writer. The sender callback will
// receive []byte arguments of length at most WriteBufferSize. All calls to the
// sender will be synchronized via the mutex.
func NewSyncWriter(m *sync.Mutex, sender func(p []byte) error) io.Writer {
	return &sendWriter{
		sender: func(p []byte) error {
			m.Lock()
			defer m.Unlock()

			return sender(p)
		},
	}
}

// WriteBufferSize is the largest []byte that Write() will pass to its
// underlying send function. This value can be changed at runtime using
// the GITALY_STREAMIO_WRITE_BUFFER_SIZE environment variable.
var WriteBufferSize = 128 * 1024

type sendWriter struct {
	sender func([]byte) error
}

func (sw *sendWriter) Write(p []byte) (int, error) {
	countMethod("writer.Write")

	var sent int

	for len(p) > 0 {
		chunkSize := len(p)
		if chunkSize > WriteBufferSize {
			chunkSize = WriteBufferSize
		}

		if err := sw.sender(p[:chunkSize]); err != nil {
			return sent, err
		}

		sent += chunkSize
		p = p[chunkSize:]
	}

	return sent, nil
}

// ReadFrom implements io.ReaderFrom.
func (sw *sendWriter) ReadFrom(r io.Reader) (int64, error) {
	countMethod("writer.ReadFrom")

	var nRead int64
	buf := make([]byte, WriteBufferSize)

	for {
		n, err := r.Read(buf)
		nRead += int64(n)

		if n > 0 {
			if err := sw.sender(buf[:n]); err != nil {
				return nRead, err
			}
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				return nRead, nil
			}
			return nRead, err
		}
	}
}
