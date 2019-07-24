package proxy

import (
	"errors"
	"fmt"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// StreamPeeker abstracts away the gRPC stream being forwarded so that it can
// be inspected and modified.
type StreamPeeker interface {
	// Peek allows a director to peak n-messages into the stream without
	// removing those messages from the stream that will be forwarded to
	// the backend server.
	Peek(ctx context.Context, n uint) (frames [][]byte, _ error)
	ReplaceFrames(n int, payloads ...[]byte) error
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

func (p peeker) Peek(ctx context.Context, n uint) ([][]byte, error) {
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

func (p peeker) ReplaceFrames(n int, payloads ...[]byte) error {
	if n > len(payloads) {
		return fmt.Errorf("not enough payloads for %d frames", n)
	}

	for i := 0; i < n; i++ {
		p.consumedStream.frames[i].payload = payloads[i]
	}

	return nil
}
