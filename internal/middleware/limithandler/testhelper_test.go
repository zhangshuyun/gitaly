package limithandler_test

import (
	"context"
	"sync/atomic"

	pb "gitlab.com/gitlab-org/gitaly/v14/internal/middleware/limithandler/testdata"
)

type server struct {
	requestCount uint64
	blockCh      chan (struct{})
}

func (s *server) registerRequest() {
	atomic.AddUint64(&s.requestCount, 1)
}

func (s *server) getRequestCount() int {
	return int(atomic.LoadUint64(&s.requestCount))
}

func (s *server) Unary(ctx context.Context, in *pb.UnaryRequest) (*pb.UnaryResponse, error) {
	s.registerRequest()

	<-s.blockCh // Block to ensure concurrency

	return &pb.UnaryResponse{Ok: true}, nil
}

func (s *server) StreamOutput(in *pb.StreamOutputRequest, stream pb.Test_StreamOutputServer) error {
	s.registerRequest()

	<-s.blockCh // Block to ensure concurrency

	return stream.Send(&pb.StreamOutputResponse{Ok: true})
}

func (s *server) StreamInput(stream pb.Test_StreamInputServer) error {
	// Read all the input
	for {
		if _, err := stream.Recv(); err != nil {
			break
		}

		s.registerRequest()
	}

	<-s.blockCh // Block to ensure concurrency

	return stream.SendAndClose(&pb.StreamInputResponse{Ok: true})
}

func (s *server) Bidirectional(stream pb.Test_BidirectionalServer) error {
	// Read all the input
	for {
		if _, err := stream.Recv(); err != nil {
			break
		}

		s.registerRequest()
	}

	<-s.blockCh // Block to ensure concurrency

	return stream.Send(&pb.BidirectionalResponse{Ok: true})
}
