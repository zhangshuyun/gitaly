// Copyright 2017 Michal Witkowski. All Rights Reserved.
// See LICENSE for licensing terms.

// TODO: remove the following linter override when the deprecations are fixed
// in issue https://gitlab.com/gitlab-org/gitaly/issues/1663
//lint:file-ignore SA1019 Ignore all gRPC deprecations until issue #1663

package proxy

import (
	"context"
	"io"

	"gitlab.com/gitlab-org/gitaly/internal/middleware/sentryhandler"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	clientStreamDescForProxying = &grpc.StreamDesc{
		ServerStreams: true,
		ClientStreams: true,
	}
)

// RegisterStreamHandlers sets up stream handlers for a set of gRPC methods for a given service.
// streamers is a map of method to grpc.StreamHandler eg:
//
// streamHandler := func(srv interface{}, stream ServerStream) error  {
//                       /** do some stuff **/
//                       return nil
//                  }
// RegisterStreamHandlers(grpcServer, "MyGrpcService", map[string]grpc.StreamHandler{"Method1": streamHandler})
// note: multiple calls with the same serviceName will result in a fatal
func RegisterStreamHandlers(server *grpc.Server, serviceName string, streamers map[string]grpc.StreamHandler) {
	desc := &grpc.ServiceDesc{
		ServiceName: serviceName,
		HandlerType: (*interface{})(nil),
	}

	for methodName, streamer := range streamers {
		streamDesc := grpc.StreamDesc{
			StreamName:    methodName,
			Handler:       streamer,
			ServerStreams: true,
			ClientStreams: true,
		}
		desc.Streams = append(desc.Streams, streamDesc)
	}

	server.RegisterService(desc, struct{}{})
}

// RegisterService sets up a proxy handler for a particular gRPC service and method.
// The behaviour is the same as if you were registering a handler method, e.g. from a codegenerated pb.go file.
//
// This can *only* be used if the `server` also uses grpcproxy.CodecForServer() ServerOption.
func RegisterService(server *grpc.Server, director StreamDirector, serviceName string, methodNames ...string) {
	streamer := &handler{director}
	fakeDesc := &grpc.ServiceDesc{
		ServiceName: serviceName,
		HandlerType: (*interface{})(nil),
	}
	for _, m := range methodNames {
		streamDesc := grpc.StreamDesc{
			StreamName:    m,
			Handler:       streamer.handler,
			ServerStreams: true,
			ClientStreams: true,
		}
		fakeDesc.Streams = append(fakeDesc.Streams, streamDesc)
	}
	server.RegisterService(fakeDesc, streamer)
}

// TransparentHandler returns a handler that attempts to proxy all requests that are not registered in the server.
// The indented use here is as a transparent proxy, where the server doesn't know about the services implemented by the
// backends. It should be used as a `grpc.UnknownServiceHandler`.
//
// This can *only* be used if the `server` also uses grpcproxy.CodecForServer() ServerOption.
func TransparentHandler(director StreamDirector) grpc.StreamHandler {
	streamer := &handler{director}
	return streamer.handler
}

type handler struct {
	director StreamDirector
}

type streamAndMsg struct {
	grpc.ClientStream
	msg []byte
}

// handler is where the real magic of proxying happens.
// It is invoked like any gRPC server stream and uses the gRPC server framing to get and receive bytes from the wire,
// forwarding it to a ClientStream established against the relevant ClientConn.
func (s *handler) handler(srv interface{}, serverStream grpc.ServerStream) error {
	// little bit of gRPC internals never hurt anyone
	fullMethodName, ok := grpc.MethodFromServerStream(serverStream)
	if !ok {
		return status.Errorf(codes.Internal, "lowLevelServerStream not exists in context")
	}

	peeker := newPeeker(serverStream)

	// We require that the director's returned context inherits from the serverStream.Context().
	params, err := s.director(serverStream.Context(), fullMethodName, peeker)
	if err != nil {
		return err
	}

	defer params.RequestFinalizer()

	clientCtx, clientCancel := context.WithCancel(params.PrimaryConn().Ctx)
	defer clientCancel()
	// TODO(mwitkow): Add a `forwarded` header to metadata, https://en.wikipedia.org/wiki/X-Forwarded-For.

	primaryClientStream, err := grpc.NewClientStream(clientCtx, clientStreamDescForProxying, params.PrimaryConn().Conn, fullMethodName, params.CallOptions()...)
	if err != nil {
		return err
	}

	var allClientStreams []streamAndMsg
	for _, conn := range params.SecondaryConns() {
		secondaryClientStream, err := grpc.NewClientStream(conn.Ctx, clientStreamDescForProxying, conn.Conn, fullMethodName, params.CallOptions()...)
		if err != nil {
			return err
		}
		allClientStreams = append(allClientStreams, streamAndMsg{
			ClientStream: secondaryClientStream,
			msg:          conn.Msg,
		})
	}

	allClientStreams = append(allClientStreams, streamAndMsg{
		ClientStream: primaryClientStream,
		msg:          params.PrimaryConn().Msg,
	})

	// Explicitly *do not close* s2cErrChan and c2sErrChan, otherwise the select below will not terminate.
	// Channels do not have to be closed, it is just a control flow mechanism, see

	// https://groups.google.com/forum/#!msg/golang-nuts/pZwdYRGxCIk/qpbHxRRPJdUJ

	s2cErrChan, s2cDoneChan := s.forwardServerToClients(serverStream, allClientStreams)
	c2sErrChan := s.forwardClientToServer(primaryClientStream, serverStream)

	// We don't know which side is going to stop sending first, so we need a select between the two.
	for i := 0; i < 2; i++ {
		select {
		case <-s2cDoneChan:
			// this is the happy case where the sender has encountered io.EOF, and won't be sending anymore./
			// the clientStream>serverStream may continue pumping though.
			for _, clientStream := range allClientStreams {
				clientStream.CloseSend()
			}
		case s2cErr := <-s2cErrChan:
			// however, we may have gotten a receive error (stream disconnected, a read error etc) in which case we need
			// to cancel the clientStream to the backend, let all of its goroutines be freed up by the CancelFunc and
			// exit with an error to the stack
			clientCancel()
			return status.Errorf(codes.Internal, "failed proxying s2c: %v", s2cErr)
		case c2sErr := <-c2sErrChan:
			// This happens when the clientStream has nothing else to offer (io.EOF), returned a gRPC error. In those two
			// cases we may have received Trailers as part of the call. In case of other errors (stream closed) the trailers
			// will be nil.
			trailer := primaryClientStream.Trailer()
			serverStream.SetTrailer(trailer)
			// c2sErr will contain RPC error from client code. If not io.EOF return the RPC error as server stream error.
			if c2sErr != io.EOF {
				if trailer != nil {
					// we must not propagate Gitaly errors into Sentry
					sentryhandler.MarkToSkip(serverStream.Context())
				}
				return c2sErr
			}
			return nil
		}
	}
	return status.Errorf(codes.Internal, "gRPC proxying should never reach this stage.")
}

func (s *handler) forwardClientToServer(src grpc.ClientStream, dst grpc.ServerStream) chan error {
	ret := make(chan error, 1)
	go func() {
		f := &frame{}
		for i := 0; ; i++ {
			if err := src.RecvMsg(f); err != nil {
				ret <- err // this can be io.EOF which is happy case
				break
			}
			if i == 0 {
				// This is a bit of a hack, but client to server headers are only readable after first client msg is
				// received but must be written to server stream before the first msg is flushed.
				// This is the only place to do it nicely.
				md, err := src.Header()
				if err != nil {
					ret <- err
					break
				}
				if err := dst.SendHeader(md); err != nil {
					ret <- err
					break
				}
			}
			if err := dst.SendMsg(f); err != nil {
				ret <- err
				break
			}
		}
	}()
	return ret
}

func (s *handler) forwardServerToClients(src grpc.ServerStream, dsts []streamAndMsg) (chan error, chan struct{}) {
	ret := make(chan error, len(dsts))
	done := make(chan struct{})

	go func() {
		var g errgroup.Group
		for _, dst := range dsts {
			dst := dst
			g.Go(func() error {
				newPayloads := [][]byte{dst.msg}
				for _, payload := range newPayloads {
					if err := dst.SendMsg(&frame{payload: payload}); err != nil {
						return err
					}
				}

				return nil
			})
		}

		if err := g.Wait(); err != nil {
			ret <- err
		}

		var frameChans []chan *frame
		for _, dst := range dsts {
			dst := dst
			frameChan := make(chan *frame)
			frameChans = append(frameChans, frameChan)
			g.Go(func() error {
				for {
					f := <-frameChan
					if f == nil {
						return nil
					}
					if err := dst.SendMsg(f); err != nil {
						return err
					}
				}
			})
		}
		// resume two-way stream after peeked messages
		f := &frame{}
		for i := 0; ; i++ {
			if err := src.RecvMsg(f); err != nil {
				for _, frameChan := range frameChans {
					close(frameChan)
				}
				if err != io.EOF {
					ret <- err // this can be io.EOF which is happy case
				}
				break
			}

			for _, frameChan := range frameChans {
				frameChan <- f
			}
		}

		if err := g.Wait(); err != nil {
			ret <- err
		}

		done <- struct{}{}
	}()
	return ret, done
}
