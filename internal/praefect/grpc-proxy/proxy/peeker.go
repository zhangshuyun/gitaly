package proxy

import (
	"errors"

	"google.golang.org/grpc"
)

// StreamPeeker abstracts away the gRPC stream being forwarded so that it can
// be inspected and modified.
type StreamPeeker interface {
	// Peek allows a director to peek a messages into the stream without
	// removing those messages from the stream that will be forwarded to
	// the backend server.
	Peek() (Frame, error)
}

// Frame contains a payload that can be optionally modified
type Frame interface {
	Modify(payload []byte) error
	Payload() []byte
}

type partialStream struct {
	frames []*frame // frames encountered in partial stream
	err    error    // error returned by partial stream
}

type peeker struct {
	srcStream      grpc.ServerStream
	consumedStream *partialStream
}

func newPeeker(stream grpc.ServerStream) *peeker {
	return &peeker{
		srcStream:      stream,
		consumedStream: &partialStream{},
	}
}

// ErrInvalidPeekCount indicates the director function requested an invalid
// peek quanity
var ErrInvalidPeekCount = errors.New("peek count must be greater than zero")

func (p peeker) Peek() (Frame, error) {
	frames, err := p.peek(1)
	if err != nil {
		return nil, err
	}

	if len(frames) != 1 {
		return nil, errors.New("failed to peek 1 message")
	}

	return frames[0], nil
}

func (p peeker) peek(n uint) ([]*frame, error) {
	if n < 1 {
		return nil, ErrInvalidPeekCount
	}

	p.consumedStream.frames = make([]*frame, n)

	for i := 0; i < len(p.consumedStream.frames); i++ {
		f := &frame{}
		if err := p.srcStream.RecvMsg(f); err != nil {
			p.consumedStream.err = err
			break
		}
		p.consumedStream.frames[i] = f
	}

	return p.consumedStream.frames, nil
}

func (f *frame) Payload() []byte {
	return f.payload
}

func (f *frame) Modify(payload []byte) error {
	f.Lock()
	defer f.Unlock()

	if f.consumed {
		return errors.New("frame has already been consumed")
	}

	if f.payload == nil {
		return errors.New("frame payload is empty")
	}

	f.payload = payload
	return nil
}
