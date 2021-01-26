package transaction

import (
	"context"
	"fmt"
	"sync"

	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type route string

type server struct {
	// requestPaths are how a Gitaly request gets routed to the correct
	// Praefect
	requestPaths struct {
		sync.RWMutex
		pathByUUID map[route]chan<- *gitalypb.RouteVoteRequest
	}

	// responsePaths are how a Praefect response gets routed to the correct
	// Gitaly RPC call
	responsePaths struct {
		sync.RWMutex
		pathByUUID map[route]chan<- *gitalypb.RouteVoteRequest
	}
}

// NewServer returns a Gitaly transaction server capable of routing votes to a
// Praefect client
func NewServer() gitalypb.RefTransactionServer {
	s := &server{}
	s.requestPaths.pathByUUID = map[route]chan<- *gitalypb.RouteVoteRequest{}
	s.responsePaths.pathByUUID = map[route]chan<- *gitalypb.RouteVoteRequest{}
	return s
}

// VoteTransaction will attempt to route the request to a listening Praefect
// client and then wait for a response to be routed back.
func (s *server) VoteTransaction(ctx context.Context, req *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
	wrappedReq := &gitalypb.RouteVoteRequest{
		RouteUuid: req.GetRouteUuid(),
		Msg:       &gitalypb.RouteVoteRequest_VoteTxRequest{req},
	}

	resp, err := s.routeRPC(ctx, wrappedReq)
	if err != nil {
		return nil, status.Errorf(status.Code(err), "VoteTransaction: routing RPC: %v", err)
	}

	switch r := resp.Msg.(type) {
	case *gitalypb.RouteVoteRequest_VoteTxResponse:
		return r.VoteTxResponse, nil
	case *gitalypb.RouteVoteRequest_Error:
		return nil, status.New(codes.Code(r.Error.Code), r.Error.Message).Err()
	default:
		return nil, helper.ErrInternalf("VoteTransaction: unexpected response type %T", resp.Msg)
	}
}

// StopTransaction will attempt to route the request to a listening Praefect
// client and then wait for a response to be routed back.
func (s *server) StopTransaction(ctx context.Context, req *gitalypb.StopTransactionRequest) (*gitalypb.StopTransactionResponse, error) {
	wrappedReq := &gitalypb.RouteVoteRequest{
		RouteUuid: req.GetRouteUuid(),
		Msg:       &gitalypb.RouteVoteRequest_StopTxRequest{req},
	}

	resp, err := s.routeRPC(ctx, wrappedReq)
	if err != nil {
		return nil, status.Errorf(status.Code(err), "StopTransaction: routing RPC: %v", err)
	}

	switch r := resp.Msg.(type) {
	case *gitalypb.RouteVoteRequest_StopTxResponse:
		return r.StopTxResponse, nil
	case *gitalypb.RouteVoteRequest_Error:
		return nil, status.New(codes.Code(r.Error.Code), r.Error.Message).Err()
	default:
		return nil, helper.ErrInternalf("StopTransaction: unexpected response type %T", resp.Msg)
	}
}

func (s *server) routeRPC(ctx context.Context, req *gitalypb.RouteVoteRequest) (*gitalypb.RouteVoteRequest, error) {
	respPath := make(chan *gitalypb.RouteVoteRequest)
	defer close(respPath)

	routeUUID := route(req.GetRouteUuid())

	cleanup, err := s.openRespPath(respPath, routeUUID)
	if err != nil {
		return nil, status.Errorf(status.Code(err), "opening response path: %v", err)
	}
	defer cleanup()

	if err := s.routeRequest(ctx, routeUUID, req); err != nil {
		return nil, status.Errorf(status.Code(err), "routing request: %v", err)
	}

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("waiting for response: %w", ctx.Err())
	case resp, ok := <-respPath:
		if !ok {
			return nil, helper.ErrInternalf("response route unexpectedly closed")
		}
		return resp, nil
	}
}

func (s *server) RouteVote(bidi gitalypb.RefTransaction_RouteVoteServer) error {
	ctx, cancel := context.WithCancel(bidi.Context())
	defer cancel()

	reqPath := make(chan *gitalypb.RouteVoteRequest)
	defer close(reqPath)

	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error { return s.processOutgoing(ctx, reqPath, bidi) })
	eg.Go(func() error { return s.processIncoming(ctx, reqPath, bidi) })

	return eg.Wait()
}

// processOutgoing handles receiving messages from other clients that need to be
// delivered to the caller of this stream
func (s *server) processOutgoing(ctx context.Context, reqPath <-chan *gitalypb.RouteVoteRequest, stream gitalypb.RefTransaction_RouteVoteServer) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-reqPath:
			if !ok {
				return nil
			}
			if err := stream.Send(msg); err != nil {
				return err
			}
		}
	}
}

// processIncoming handles each incoming message to this stream
func (s *server) processIncoming(ctx context.Context, reqPath chan<- *gitalypb.RouteVoteRequest, bidi gitalypb.RefTransaction_RouteVoteServer) error {
	var sessionOpened bool
	for {
		var req *gitalypb.RouteVoteRequest
		var err error

		// there is no way to cancel early from a stream recv, so we
		// wrap it in a goroutine to select it against context cancelation
		done := make(chan struct{})
		go func() {
			defer close(done)
			req, err = bidi.Recv() // should return when RPC is done
		}()

		select {
		case <-done:
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}

		routeUUID := route(req.GetRouteUuid())

		switch req.GetMsg().(type) {
		case *gitalypb.RouteVoteRequest_OpenRouteRequest:
			if sessionOpened {
				return status.Error(codes.AlreadyExists, "route already exists for stream")
			}
			if err := s.openReqPath(reqPath, routeUUID, bidi); err != nil {
				return err
			}
			sessionOpened = true
			defer s.closeRoute(routeUUID)
		default:
			if err := s.routeResponse(ctx, routeUUID, req); err != nil {
				return err
			}
		}
	}
}

func (s *server) closeRoute(routeUUID route) {
	s.requestPaths.Lock()
	delete(s.requestPaths.pathByUUID, routeUUID)
	s.requestPaths.Unlock()
}

func (s *server) openReqPath(reqPath chan<- *gitalypb.RouteVoteRequest, routeID route, bidi gitalypb.RefTransaction_RouteVoteServer) error {
	s.requestPaths.Lock()
	defer s.requestPaths.Unlock()

	_, ok := s.requestPaths.pathByUUID[routeID]
	if ok {
		return status.Errorf(codes.AlreadyExists, "route session already exists for %s", routeID)
	}

	s.requestPaths.pathByUUID[routeID] = reqPath

	// send back same message as confirmation of session
	// creation
	return bidi.Send(&gitalypb.RouteVoteRequest{
		RouteUuid: string(routeID),
		Msg: &gitalypb.RouteVoteRequest_OpenRouteRequest{
			OpenRouteRequest: &gitalypb.RouteVoteRequest_OpenRoute{},
		},
	})
}

func (s *server) openRespPath(respPath chan<- *gitalypb.RouteVoteRequest, routeID route) (func(), error) {
	s.responsePaths.Lock()
	defer s.responsePaths.Unlock()

	_, ok := s.responsePaths.pathByUUID[routeID]
	if ok {
		return func() {}, status.Errorf(codes.AlreadyExists, "route session already exists for %s", routeID)
	}

	s.responsePaths.pathByUUID[routeID] = respPath

	cleanup := func() {
		s.responsePaths.Lock()
		delete(s.responsePaths.pathByUUID, routeID)
		s.responsePaths.Unlock()
	}

	return cleanup, nil
}

// routeRequest attempts to find a route for the given route ID and send the
// request message to the route owner
func (s *server) routeRequest(ctx context.Context, routeID route, req *gitalypb.RouteVoteRequest) error {
	s.requestPaths.Lock()
	reqPath, ok := s.requestPaths.pathByUUID[routeID]
	s.requestPaths.Unlock()

	if !ok {
		return status.Errorf(codes.NotFound, "route does not exist for UUID %s", req.RouteUuid)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case reqPath <- req:
		return nil
	}
}

// routeResponse attempts to find a route for the given route ID and send the
// response message to the Gitaly caller
func (s *server) routeResponse(ctx context.Context, routeID route, resp *gitalypb.RouteVoteRequest) error {
	s.responsePaths.RLock()
	respPath, ok := s.responsePaths.pathByUUID[routeID]
	s.responsePaths.RUnlock()

	if !ok {
		return status.Errorf(codes.NotFound, "route does not exist for UUID %s", resp.RouteUuid)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case respPath <- resp:
		return nil
	}
}
