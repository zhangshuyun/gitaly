package health

import (
	"context"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"

	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func (s *server) Check(ctx context.Context, in *gitalypb.HealthCheckRequest) (*gitalypb.HealthCheckResponse, error) {
	if s.errorTracker.ReadErrs() > 1000 || s.errorTracker.WriteErrs() > 100 {
		ctxlogrus.Extract(ctx).Info("I'm not healthy 😵")
		return &gitalypb.HealthCheckResponse{Status: gitalypb.HealthCheckResponse_NOT_SERVING}, nil
	}
	ctxlogrus.Extract(ctx).Info("I'm healthy 😁")
	return &gitalypb.HealthCheckResponse{Status: gitalypb.HealthCheckResponse_SERVING}, nil
}

func (s *server) Watch(in *gitalypb.HealthCheckRequest, stream gitalypb.Health_WatchServer) error {
	ticker := time.NewTicker(10 * time.Second)

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-ticker.C:
			if s.errorTracker.ReadErrs() > 1000 || s.errorTracker.WriteErrs() > 100 {
				if err := stream.Send(&gitalypb.HealthCheckResponse{Status: gitalypb.HealthCheckResponse_NOT_SERVING}); err != nil {
					return err
					continue
				}
				if err := stream.Send(&gitalypb.HealthCheckResponse{Status: gitalypb.HealthCheckResponse_SERVING}); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
