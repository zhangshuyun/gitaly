package streamrpc

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"time"
)

type request struct {
	Method   string
	Message  []byte
	Metadata map[string][]string
}

type response struct{ Error string }

const (
	defaultHandshakeTimeout = 10 * time.Second

	// The frames exchanged during the handshake have a uint32 length prefix
	// so their theoretical maximum size is 4GB. We don't want to allow that
	// so we enforce a lower limit. This number was chosen because it is
	// close to the default grpc-go maximum message size.
	maxFrameSize = (1 << 20) - 1
)

var (
	errFrameTooLarge = errors.New("frame too large")
)

func sendFrame(c net.Conn, frame []byte, deadline time.Time) error {
	if len(frame) > maxFrameSize {
		return errFrameTooLarge
	}

	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(frame)))
	if err := errAsync(deadline, func() (int, error) { return c.Write(header) }); err != nil {
		return err
	}

	return errAsync(deadline, func() (int, error) { return c.Write(frame) })
}

func recvFrame(c net.Conn, deadline time.Time) ([]byte, error) {
	header := make([]byte, 4)
	if err := errAsync(deadline, func() (int, error) { return io.ReadFull(c, header) }); err != nil {
		return nil, err
	}

	size := binary.BigEndian.Uint32(header)
	if size > maxFrameSize {
		return nil, errFrameTooLarge
	}
	frame := make([]byte, size)
	if err := errAsync(deadline, func() (int, error) { return io.ReadFull(c, frame) }); err != nil {
		return nil, err
	}

	return frame, nil
}

// errAsync is a hack to work around the fact that grpc-go calls
// SetDeadline on connections _after_ handing them over to us. Because of
// this race, we cannot use SetDeadline which would have been nicer.
//
// https://github.com/grpc/grpc-go/blob/v1.38.0/server.go#L853
func errAsync(deadline time.Time, f func() (int, error)) error {
	tm := time.NewTimer(time.Until(deadline))
	defer tm.Stop()

	errC := make(chan error, 1)
	go func() {
		_, err := f()
		errC <- err
	}()

	select {
	case <-tm.C:
		return os.ErrDeadlineExceeded
	case err := <-errC:
		return err
	}
}
