package streamrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

// DialFunc is an abstraction that allows Call to transparently handle
// unencrypted connections and TLS connections.
type DialFunc func(time.Duration) (net.Conn, error)

func doCall(dial DialFunc, request []byte, callback func(net.Conn) error) error {
	deadline := time.Now().Add(defaultHandshakeTimeout)

	c, err := dial(time.Until(deadline))
	if err != nil {
		return fmt.Errorf("streamrpc client: dial: %w", err)
	}
	defer c.Close()

	if err := sendFrame(c, request, deadline); err != nil {
		return fmt.Errorf("streamrpc client: send handshake: %w", err)
	}

	responseBytes, err := recvFrame(c, deadline)
	if err != nil {
		return fmt.Errorf("streamrpc client: receive handshake: %w", err)
	}

	if len(responseBytes) > 0 {
		var resp response
		if err := json.Unmarshal(responseBytes, &resp); err != nil {
			return fmt.Errorf("streamrpc client: unmarshal handshake response: %w", err)
		}

		return &RequestRejectedError{resp.Error}
	}

	if err := callback(c); err != nil {
		return err
	}

	return c.Close()
}

// RequestRejectedError is returned by Call if the server explicitly
// rejected the request (as opposed to e.g. an IO timeout).
type RequestRejectedError struct{ message string }

func (r *RequestRejectedError) Error() string { return r.message }

type callOptions struct {
	creds       credentials.PerRPCCredentials
	interceptor grpc.UnaryClientInterceptor
}

func (opts *callOptions) addCredentials(ctx context.Context) (context.Context, error) {
	headers, err := opts.creds.GetRequestMetadata(ctx)
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		ctx = metadata.AppendToOutgoingContext(ctx, k, v)
	}
	return ctx, nil
}

// CallOption is an abstraction that lets us pass 0 or more options to a call.
type CallOption func(*callOptions)

type nullCredentials struct{}

func (nullCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return nil, nil
}

func (nullCredentials) RequireTransportSecurity() bool { return false }

var _ credentials.PerRPCCredentials = nullCredentials{}

func nullClientInterceptor(ctx context.Context, method string, req, resp interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	return invoker(ctx, method, req, resp, cc, opts...)
}

func defaultCallOptions() *callOptions {
	return &callOptions{
		creds:       nullCredentials{},
		interceptor: nullClientInterceptor,
	}
}

// WithCredentials adds gRPC per-request credentials to an outgoing call.
func WithCredentials(creds credentials.PerRPCCredentials) CallOption {
	return func(o *callOptions) { o.creds = creds }
}

// WithClientInterceptor adds a gRPC unary client interceptor to an outgoing call.
func WithClientInterceptor(interceptor grpc.UnaryClientInterceptor) CallOption {
	return func(o *callOptions) { o.interceptor = interceptor }
}

// Call makes a StreamRPC call. The dial function determines the remote
// address. The method argument is the full name of the StreamRPC method
// we are calling (e.g. "/foo.BarService/BazStream"). Msg is the request
// message. If the server accepts the call, callback is called with a
// connection.
func Call(ctx context.Context, dial DialFunc, method string, msg proto.Message, callback func(net.Conn) error, opts ...CallOption) (err error) {
	callOpts := defaultCallOptions()
	for _, o := range opts {
		o(callOpts)
	}

	invoke := func(ctx context.Context, method string, msg, _ interface{}, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		ctx, err := callOpts.addCredentials(ctx)
		if err != nil {
			return fmt.Errorf("streamrpc client: add credentials: %w", err)
		}

		msgBytes, err := proto.Marshal(msg.(proto.Message))
		if err != nil {
			return fmt.Errorf("streamrpc client: marshal outgoing protobuf message: %w", err)
		}

		req := &request{
			Method:  method,
			Message: msgBytes,
		}
		if md, ok := metadata.FromOutgoingContext(ctx); ok {
			req.Metadata = md
		}

		reqBytes, err := json.Marshal(req)
		if err != nil {
			return fmt.Errorf("streamrpc client: marshal request json: %w", err)
		}

		return doCall(dial, reqBytes, callback)
	}

	return callOpts.interceptor(ctx, method, msg, nil, nil, invoke)
}
