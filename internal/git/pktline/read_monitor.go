package pktline

import (
	"bytes"
	"context"
	"io"
	"os"
	"time"
)

// ReadMonitor monitors an io.Reader, waiting for a specified packet. If the
// packet doesn't come within a timeout, a cancel function is called. This can
// be used to place a timeout on the *negotiation* phase of some git commands,
// aborting them if it is exceeded.
//
// This timeout prevents a class of "use-after-check" security issue when the
// access check for a git command is run before the command itself. The user
// has control of stdin for the git command, and if they can delay input for
// an arbitrarily long time, they can gain access days or weeks after the
// access check has completed.
//
// This approach is better than placing a timeout on the overall git operation
// because there is a conflict between mitigating the use-after-check with a
// short timeout, and allowing long-lived git operations to complete. The
// negotiation phase is a small proportion of the time taken for a large git
// fetch, for instance, so tighter limits can be placed on it, leading to a
// better mitigation.
type ReadMonitor struct {
	pr         *os.File
	pw         *os.File
	underlying io.Reader
}

// NewReadMonitor wraps the provided reader with an os.Pipe(), returning the
// read end for onward use.
//
// Call Monitor(pkt, timeout, cancelFn) to start streaming from the reader to
// to the pipe. The stream will be monitored for a pktline-formatted packet
// matching pkt. If it isn't seen within the timeout, cancelFn will be called.
//
// Resources will be freed when the context is done, but you should close the
// returned *os.File earlier if possible.
func NewReadMonitor(ctx context.Context, r io.Reader) (*os.File, *ReadMonitor, error) {
	pr, pw, err := os.Pipe()
	if err != nil {
		return nil, nil, err
	}

	// Ensure all resources are closed once the context is done
	go func() {
		<-ctx.Done()

		pr.Close()
		pw.Close()
	}()

	return pr, &ReadMonitor{
		pr:         pr,
		pw:         pw,
		underlying: r,
	}, nil
}

// Monitor should be called at most once. It scans the stream, looking for the
// specified packet, and will call cancelFn if it isn't seen within the timeout
func (m *ReadMonitor) Monitor(pkt []byte, timeout time.Duration, cancelFn func()) {
	timer := time.AfterFunc(timeout, cancelFn)
	teeReader := io.TeeReader(m.underlying, m.pw)

	scanner := NewScanner(teeReader)
	for scanner.Scan() {
		if bytes.Equal(scanner.Bytes(), pkt) {
			timer.Stop()
			break
		}
	}

	// Complete the read loop, then signal completion on pr by closing pw
	_, _ = io.Copy(io.Discard, teeReader)
	_ = m.pw.Close()
}
