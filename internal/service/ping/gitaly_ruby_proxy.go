package notifications

import (
	"golang.org/x/net/context"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
)

func (s *server) pingGitalyRuby(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	client, err := s.PingServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	return client.Ping(ctx, req)
}

func (s *server) pingServerStreamGitalyRuby(req *pb.PingRequest, stream pb.PingService_PingServerStreamServer) error {
	return nil
}
