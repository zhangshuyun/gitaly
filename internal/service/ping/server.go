package notifications

import (
	"io"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
)

type server struct {
	*rubyserver.Server
}

func getSingleValue(ctx context.Context, key string) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}

	routing := md[key]
	if len(routing) == 1 {
		return routing[0]
	}

	return ""
}

func (s *server) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	routing := getSingleValue(ctx, "routing")

	if routing == "gitaly-ruby" {
		return s.pingGitalyRuby(ctx, req)
	}

	wantedResponse := getSingleValue(ctx, "response")
	if wantedResponse == "exception" {
		return nil, grpc.Errorf(codes.InvalidArgument, "Error")
	}

	return &pb.PingResponse{ClientTime: req.ClientTime}, nil
}

func (s *server) PingServerStream(req *pb.PingRequest, stream pb.PingService_PingServerStreamServer) error {
	ctx := stream.Context()
	routing := getSingleValue(ctx, "routing")

	if routing == "gitaly-ruby" {
		return s.pingServerStreamGitalyRuby(req, stream)
	}

	wantedResponse := getSingleValue(ctx, "response")
	if wantedResponse == "exception" {
		return grpc.Errorf(codes.InvalidArgument, "Error")
	}

	for i := 0; i < 10; i++ {
		stream.Send(&pb.PingResponse{ClientTime: req.ClientTime})
	}

	return nil
}

func (s *server) PingClientStream(stream pb.PingService_PingClientStreamServer) error {
	ctx := stream.Context()
	routing := getSingleValue(ctx, "routing")

	if routing == "gitaly-ruby" {
		return s.pingClientStreamGitalyRuby(stream)
	}

	wantedResponse := getSingleValue(ctx, "response")
	if wantedResponse == "exception" {
		return grpc.Errorf(codes.InvalidArgument, "Error")
	}

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}
	}
	return nil
}

func (s *server) PingBidiStream(stream pb.PingService_PingBidiStreamServer) error {
	ctx := stream.Context()
	routing := getSingleValue(ctx, "routing")

	if routing == "gitaly-ruby" {
		return s.pingClientBidiGitalyRuby(stream)
	}

	wantedResponse := getSingleValue(ctx, "response")
	if wantedResponse == "exception" {
		return grpc.Errorf(codes.InvalidArgument, "Error")
	}

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		err = stream.Send(&pb.PingResponse{ClientTime: req.ClientTime})
		if err {
			return err
		}
	}
	return nil
}

// NewServer creates a new instance of a grpc PingService Server
func NewServer(rs *rubyserver.Server) pb.PingServiceServer {
	return &server{rs}
}
