package streamcache

import (
	"errors"
	"io"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var sendfileCounter = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "gitaly_streamcache_sendfile_bytes_total",
		Help: "Number of bytes sent using sendfile",
	},
)

func (pr *pipeReader) WriteTo(w io.Writer) (int64, error) {
	if n, err := pr.writeTo(w); n > 0 || err == nil {
		return n, err
	}

	// If n == 0 and err != nil then we were unable to use sendfile(2), so
	// try again using io.Copy and Read. Use struct{ io.Reader } to prevent
	// infinite recursion into pr.WriteTo().
	return io.Copy(w, struct{ io.Reader }{pr})
}

// writeTo tries to copy the pipe stream to w using the Linux sendfile(2)
// system call. This may fail for various reasons (type of w, type of
// pr.reader, Linux kernel version). If writeTo returns 0 and a non-nil
// error, the caller should try again with io.Copy and pipeReader.Read().
func (pr *pipeReader) writeTo(w io.Writer) (int64, error) {
	// If w does not have a file descriptor (maybe it is a TLS connection, or
	// a *bytes.Buffer), this first step will fail.
	dst, err := getRawconn(w)
	if err != nil {
		return 0, err
	}

	// pr.reader must also be a thing with a file descriptor.
	src, err := getRawconn(pr.reader)
	if err != nil {
		return 0, err
	}

	start := pr.position
	var errRead, errWrite, errSendfile error

	// src.Read gives us the file descriptor of the underlying file of the
	// pipe.
	errRead = src.Read(func(srcFd uintptr) bool {
		// dst.Write gives us the file descriptor of the thing we write into,
		// typically a network socket.
		errWrite = dst.Write(func(dstFd uintptr) bool {
			errSendfile = pr.sendfile(int(dstFd), int(srcFd))

			// If errSendfile is EAGAIN, ask Go runtime to wait for dst to become
			// writeable again by returning false.
			return errSendfile != syscall.EAGAIN
		})

		return true
	})
	written := pr.position - start

	for _, err := range []error{errRead, errWrite, errSendfile} {
		if err != nil {
			return written, err
		}
	}

	return written, nil
}

func getRawconn(v interface{}) (syscall.RawConn, error) {
	if sc, ok := v.(syscall.Conn); ok {
		return sc.SyscallConn()
	}

	return nil, errors.New("value does not implement syscall.Conn")
}

func (pr *pipeReader) sendfile(dst int, src int) error {
	for {
		// There is no point in calling sendfile if we already know there is no
		// unread data in the underlying file of the pipe. So let's wait for the
		// pipe to tell us there is data.
		if !pr.waitReadable() {
			// We are at EOF.
			return nil
		}

		// We need to give sendfile a maximum number of bytes to copy. It is OK
		// if it is too big because sendfile won't block trying to copy bytes
		// that aren't in the file. We picked 4MB because that is the same
		// maximum the Go stdlib uses.
		// https://github.com/golang/go/blob/go1.16.7/src/internal/poll/sendfile_linux.go#L11
		const maxBytes = 4 << 20
		n, err := syscall.Sendfile(dst, src, nil, maxBytes)

		// sendfile returns -1 in case of errors, so we must check if n is
		// positive.
		if n > 0 {
			pr.advancePosition(n)
			sendfileCounter.Add(float64(n))
			pr.sendfileCalledSuccessfully = true
		}

		// In case of EINTR, ignore the error and retry immediately
		if err != nil && err != syscall.EINTR {
			return err
		}
	}
}
