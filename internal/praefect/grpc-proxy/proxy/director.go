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
	primaryConn    Destination
	reqFinalizer   func()
	callOptions    []grpc.CallOption
	secondaryConns []Destination
}

// Destination contains a client connection as well as a rewritten protobuf message
type Destination struct {
	Ctx  context.Context
	Conn *grpc.ClientConn
	Msg  []byte
}

// NewStreamParameters returns a new instance of StreamParameters
func NewStreamParameters(primaryConn Destination, secondaryConns []Destination, reqFinalizer func(), callOpts []grpc.CallOption) *StreamParameters {
	return &StreamParameters{
		primaryConn:    primaryConn,
		reqFinalizer:   reqFinalizer,
		callOptions:    callOpts,
		secondaryConns: secondaryConns,
	}
}

func (s *StreamParameters) PrimaryConn() Destination {
	return s.primaryConn
}

func (s *StreamParameters) SecondaryConns() []Destination {
	return s.secondaryConns
}

// RequestFinalizer calls the request finalizer
func (s *StreamParameters) RequestFinalizer() {
	if s.reqFinalizer != nil {
		s.reqFinalizer()
	}
}

// CallOptions returns call options
func (s *StreamParameters) CallOptions() []grpc.CallOption {
	return s.callOptions
}
