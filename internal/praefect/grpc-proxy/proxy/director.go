// Copyright 2017 Michal Witkowski. All Rights Reserved.
// See LICENSE for licensing terms.

package proxy

import (
	"context"

	"google.golang.org/grpc"
)

// StreamDirector returns a gRPC ClientConn to be used to forward the call to.
//
// The presence of the `Context` allows for rich filtering, e.g. based on Metadata (headers).
// If no handling is meant to be done, a `codes.NotImplemented` gRPC error should be returned.
//
// The context returned from this function should be the context for the *outgoing* (to backend) call. In case you want
// to forward any Metadata between the inbound request and outbound requests, you should do it manually. However, you
// *must* propagate the cancel function (`context.WithCancel`) of the inbound context to the one returned.
//
// It is worth noting that the StreamDirector will be fired *after* all server-side stream interceptors
// are invoked. So decisions around authorization, monitoring etc. are better to be handled there.
//
// See the rather rich example.
type StreamDirector func(ctx context.Context, fullMethodName string, peeker StreamPeeker) (*StreamParameters, error)

// StreamParameters encapsulates streaming parameters the praefect coordinator returns to the
// proxy handler
type StreamParameters struct {
	primary      Destination
	reqFinalizer func() error
	callOptions  []grpc.CallOption
	secondaries  []Destination
}

// Destination contains a client connection as well as a rewritten protobuf message
type Destination struct {
	// Ctx is the context used for the connection.
	Ctx context.Context
	// Conn is the GRPC client connection.
	Conn *grpc.ClientConn
	// Msg is the initial message which shall be sent to the destination. This is used in order
	// to allow for re-writing the header message.
	Msg []byte
	// ErrHandler is invoked when proxying to the destination fails. It can be used to swallow
	// errors in case proxying failures are considered to be non-fatal. If all errors are
	// swallowed, the proxied RPC will be successful.
	ErrHandler func(error) error
}

// NewStreamParameters returns a new instance of StreamParameters
func NewStreamParameters(primary Destination, secondaries []Destination, reqFinalizer func() error, callOpts []grpc.CallOption) *StreamParameters {
	return &StreamParameters{
		primary:      primary,
		secondaries:  secondaries,
		reqFinalizer: reqFinalizer,
		callOptions:  callOpts,
	}
}

func (s *StreamParameters) Primary() Destination {
	return s.primary
}

func (s *StreamParameters) Secondaries() []Destination {
	return s.secondaries
}

// RequestFinalizer calls the request finalizer
func (s *StreamParameters) RequestFinalizer() error {
	if s.reqFinalizer != nil {
		return s.reqFinalizer()
	}
	return nil
}

// CallOptions returns call options
func (s *StreamParameters) CallOptions() []grpc.CallOption {
	return s.callOptions
}
