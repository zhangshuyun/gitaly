package server

import (
	"context"
	"fmt"

	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/proxy"
)

// ServerInfo sends ServerInfoRequest to all of a praefect server's internal gitaly nodes and aggregates the results into
// a response
func (s *Server) ServerInfo(ctx context.Context, in *gitalypb.ServerInfoRequest) (*gitalypb.ServerInfoResponse, error) {

	storageStatuses := make([][]*gitalypb.ServerInfoResponse_StorageStatus, len(s.conf.Nodes))

	g, ctx := errgroup.WithContext(ctx)

	for i, node := range s.conf.Nodes {
		i := i // necessary since it will be used in a goroutine below
		cc, ok := s.nodes[node.Storage]
		var err error
		if !ok {
			cc, err = client.Dial(node.Address,
				[]grpc.DialOption{
					grpc.WithDefaultCallOptions(grpc.CallCustomCodec(proxy.Codec())),
					grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials(node.Token)),
				},
			)
			if err != nil {
				return nil, helper.ErrInternal(err)
			}
			s.nodes[node.Storage] = cc
		}
		g.Go(func() error {
			client := gitalypb.NewServerServiceClient(cc)
			resp, err := client.ServerInfo(ctx, &gitalypb.ServerInfoRequest{})
			if err != nil {
				return fmt.Errorf("error when requesting server info from internal storage %v", node.Storage)
			}

			storageStatuses[i] = resp.GetStorageStatuses()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, helper.ErrInternal(err)
	}

	var response gitalypb.ServerInfoResponse

	for _, storageStatus := range storageStatuses {
		response.StorageStatuses = append(response.StorageStatuses, storageStatus...)
	}

	return &response, nil
}
