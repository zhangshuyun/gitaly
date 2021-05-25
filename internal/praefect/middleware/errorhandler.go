package middleware

import (
	"context"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/nodes/tracker"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/protoregistry"
	"google.golang.org/grpc"
)

// StreamErrorHandler returns a client interceptor that will track accessor/mutator errors from internal gitaly nodes
func StreamErrorHandler(registry *protoregistry.Registry, errorTracker tracker.ErrorTracker, nodeStorage string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		stream, err := streamer(ctx, desc, cc, method, opts...)

		mi, lookupErr := registry.LookupMethod(method)
		if err != nil {
			return nil, fmt.Errorf("error when looking up method: %w %v", err, lookupErr)
		}

		return newCatchErrorStreamer(stream, errorTracker, mi.Operation, nodeStorage), err
	}
}

// catchErrorSteamer is a custom ClientStream that adheres to grpc.ClientStream but keeps track of accessor/mutator errors
type catchErrorStreamer struct {
	grpc.ClientStream
	errors      tracker.ErrorTracker
	operation   protoregistry.OpType
	nodeStorage string
}

func newCatchErrorStreamer(streamer grpc.ClientStream, errors tracker.ErrorTracker, operation protoregistry.OpType, nodeStorage string) *catchErrorStreamer {
	return &catchErrorStreamer{
		ClientStream: streamer,
		errors:       errors,
		operation:    operation,
		nodeStorage:  nodeStorage,
	}
}

// SendMsg proxies the send but records any errors
func (c *catchErrorStreamer) SendMsg(m interface{}) error {
	err := c.ClientStream.SendMsg(m)
	if err != nil {
		switch c.operation {
		case protoregistry.OpAccessor:
			c.errors.IncrReadErr(c.nodeStorage)
		case protoregistry.OpMutator:
			c.errors.IncrWriteErr(c.nodeStorage)
		}
	}

	return err
}

// RecvMsg proxies the send but records any errors
func (c *catchErrorStreamer) RecvMsg(m interface{}) error {
	err := c.ClientStream.RecvMsg(m)
	if err != nil && err != io.EOF {
		switch c.operation {
		case protoregistry.OpAccessor:
			c.errors.IncrReadErr(c.nodeStorage)
		case protoregistry.OpMutator:
			c.errors.IncrWriteErr(c.nodeStorage)
		}
	}

	return err
}
