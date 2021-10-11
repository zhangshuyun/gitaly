package cancelhandler

import (
	"context"
	"errors"

	"github.com/hashicorp/yamux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Unary is a unary server interceptor that puts cancel codes on errors
// from canceled contexts.
func Unary(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	resp, err := handler(ctx, req)
	return resp, wrapErr(ctx, err)
}

// Stream is a stream server interceptor that puts cancel codes on errors
// from canceled contexts.
func Stream(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return wrapErr(stream.Context(), handler(srv, stream))
}

func wrapErr(ctx context.Context, err error) error {
	if err == nil {
		return err
	}

	// The gRPC may not be aware that sidechannel clients hang up. Therefore,
	// we have to handle yamux errors here. There are two errors for this scenario:
	// - The client called Close(). A flagFIN is sent to the server.
	//   + The server starts force close timer, but is still able to write
	//   + The client starts a force close timer
	// - When the timers are due, any read/write operations raise
	// ErrStreamClosed. They send flagRST flag to the other side.
	// - When either side receives flagRST, any read/write operations raise
	// ErrConnectionReset
	if errors.Is(err, yamux.ErrStreamClosed) || errors.Is(err, yamux.ErrConnectionReset) {
		return status.Errorf(codes.Canceled, "%v", err)
	}

	if ctx.Err() == nil {
		return err
	}

	code := codes.Canceled
	if ctx.Err() == context.DeadlineExceeded {
		code = codes.DeadlineExceeded
	}
	return status.Errorf(code, "%v", err)
}
