package remote

import (
	"fmt"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FetchInternalRemote fetches another Gitaly repository set as a remote
func (s *server) FetchInternalRemote(ctx context.Context, req *gitalypb.FetchInternalRemoteRequest) (*gitalypb.FetchInternalRemoteResponse, error) {
	if err := validateFetchInternalRemoteRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "FetchInternalRemote: %v", err)
	}

	client, err := s.RemoteServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, req.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.FetchInternalRemote(clientCtx, req)
}

func validateFetchInternalRemoteRequest(req *gitalypb.FetchInternalRemoteRequest) error {
	if req.GetRepository() == nil {
		return fmt.Errorf("empty Repository")
	}

	if req.GetRemoteRepository() == nil {
		return fmt.Errorf("empty Remote Repository")
	}

	return nil
}
