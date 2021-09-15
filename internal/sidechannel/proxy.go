package sidechannel

import (
	"context"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata"
	"google.golang.org/grpc"
	grpcMetadata "google.golang.org/grpc/metadata"
)

// NewUnaryProxy creates a gRPC client middleware that proxies sidechannels.
func NewUnaryProxy(registry *Registry) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if !hasSidechannelMetadata(ctx) {
			return invoker(ctx, method, req, reply, cc, opts...)
		}

		ctx, waiter := RegisterSidechannel(ctx, registry, proxy(ctx))
		defer waiter.Close()

		if err := invoker(ctx, method, req, reply, cc, opts...); err != nil {
			return err
		}
		if err := waiter.Close(); err != nil && err != ErrCallbackDidNotRun {
			return fmt.Errorf("sidechannel: proxy callback: %w", err)
		}
		return nil
	}
}

// NewStreamProxy creates a gRPC client middleware that proxies sidechannels.
func NewStreamProxy(registry *Registry) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if !hasSidechannelMetadata(ctx) {
			return streamer(ctx, desc, cc, method, opts...)
		}

		ctx, waiter := RegisterSidechannel(ctx, registry, proxy(ctx))
		go func() {
			<-ctx.Done()
			// The Close() error is checked and bubbled up in
			// streamWrapper.RecvMsg(). This call is just for cleanup.
			_ = waiter.Close()
		}()

		cs, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, err
		}

		return &streamWrapper{ClientStream: cs, waiter: waiter}, nil
	}
}

type streamWrapper struct {
	grpc.ClientStream
	waiter *Waiter
}

func (sw *streamWrapper) RecvMsg(m interface{}) error {
	if err := sw.ClientStream.RecvMsg(m); err != io.EOF {
		return err
	}

	if err := sw.waiter.Close(); err != nil && err != ErrCallbackDidNotRun {
		return fmt.Errorf("sidechannel: proxy callback: %w", err)
	}

	return io.EOF
}

func hasSidechannelMetadata(ctx context.Context) bool {
	md, ok := grpcMetadata.FromOutgoingContext(ctx)
	return ok && len(md.Get(sidechannelMetadataKey)) > 0
}

func proxy(ctx context.Context) func(*ClientConn) error {
	return func(upstream *ClientConn) error {
		downstream, err := OpenSidechannel(metadata.OutgoingToIncoming(ctx))
		if err != nil {
			return err
		}
		defer downstream.Close()

		const nStreams = 2
		errC := make(chan error, nStreams)

		go func() {
			errC <- func() error {
				if _, err := io.Copy(upstream, downstream); err != nil {
					return err
				}

				// Downstream.Read() has returned EOF. That means we are done proxying
				// the request body from downstream to upstream. Propagate this EOF to
				// upstream by calling CloseWrite(). Use CloseWrite(), not Close(),
				// because we still want to read the response body from upstream in the
				// other goroutine.
				return upstream.CloseWrite()
			}()
		}()

		go func() {
			errC <- func() error {
				if _, err := io.Copy(downstream, upstream); err != nil {
					return err
				}

				// Upstream is now closed for both reads and writes. Propagate this state
				// to downstream. This also happens via defer, but this way we can log
				// the Close error if there is one.
				return downstream.Close()
			}()
		}()

		for i := 0; i < nStreams; i++ {
			if err := <-errC; err != nil {
				return err
			}
		}

		return nil
	}
}
