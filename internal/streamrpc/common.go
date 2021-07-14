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
	buffers := net.Buffers([][]byte{header, frame})

	return errAsync(deadline, func() error { _, err := buffers.WriteTo(c); return err })
}

func recvFrame(c net.Conn, deadline time.Time) ([]byte, error) {
	header := make([]byte, 4)
	if err := errAsync(deadline, func() error { _, err := io.ReadFull(c, header); return err }); err != nil {
		return nil, err
	}

	size := binary.BigEndian.Uint32(header)
	if size > maxFrameSize {
		return nil, errFrameTooLarge
	}
	frame := make([]byte, size)
	if err := errAsync(deadline, func() error { _, err := io.ReadFull(c, frame); return err }); err != nil {
		return nil, err
	}

	return frame, nil
}

// errAsync is a hack to work around the fact that grpc-go calls
// SetDeadline on connections _after_ handing them over to us. Because of
// this race, we cannot use SetDeadline which would have been nicer.
//
// https://github.com/grpc/grpc-go/blob/v1.38.0/server.go#L853
func errAsync(deadline time.Time, f func() error) error {
	tm := time.NewTimer(time.Until(deadline))
	defer tm.Stop()

	errC := make(chan error, 1)
	go func() { errC <- f() }()

	select {
	case <-tm.C:
		return os.ErrDeadlineExceeded
	case err := <-errC:
		return err
	}
}
