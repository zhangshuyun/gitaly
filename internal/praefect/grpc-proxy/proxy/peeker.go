package proxy

import (
	"errors"
	"fmt"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// StreamModifier abstracts away the gRPC stream being forwarded so that it can
// be inspected and modified.
type StreamModifier interface {
	// Peek allows a director to peak a messages into the stream without
	// removing those messages from the stream that will be forwarded to
	// the backend server.
	Peek(ctx context.Context) (frame []byte, _ error)

	// Modify modifies a payload in the stream. It will replace a frame with the
	// payload it is given
	Modify(ctx context.Context, payload []byte) error
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

func (p peeker) Peek(ctx context.Context) ([]byte, error) {
	payloads, err := p.peek(ctx, 1)
	if err != nil {
		return nil, err
	}

	if len(payloads) != 1 {
		return nil, errors.New("failed to peek 1 message")
	}

	return payloads[0], nil
}

func (p peeker) Modify(ctx context.Context, payload []byte) error {
	return p.modify(ctx, [][]byte{payload})
}

func (p peeker) peek(ctx context.Context, n uint) ([][]byte, error) {
	if n < 1 {
		return nil, ErrInvalidPeekCount
	}

	p.consumedStream.frames = make([]*frame, n)
	peekedFrames := make([][]byte, n)

	for i := 0; i < len(p.consumedStream.frames); i++ {
		f := &frame{}
		if err := p.srcStream.RecvMsg(f); err != nil {
			p.consumedStream.err = err
			break
		}
		p.consumedStream.frames[i] = f
		peekedFrames[i] = f.payload
	}

	return peekedFrames, nil
}

func (p peeker) modify(ctx context.Context, payloads [][]byte) error {
	if len(payloads) != len(p.consumedStream.frames) {
		return fmt.Errorf("replacement frames count %d does not match consumed frames count %d", len(payloads), len(p.consumedStream.frames))
	}

	for i, payload := range payloads {
		p.consumedStream.frames[i].payload = payload
	}

	return nil
}
