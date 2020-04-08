package server

import (
	"context"
	"sync"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"golang.org/x/sync/errgroup"
)

// ServerInfo sends ServerInfoRequest to all of a praefect server's internal gitaly nodes and aggregates the results into
// a response
func (s *Server) ServerInfo(ctx context.Context, in *gitalypb.ServerInfoRequest) (*gitalypb.ServerInfoResponse, error) {
	var once sync.Once

	var nodes []nodes.Node
	for _, virtualStorage := range s.conf.VirtualStorages {
		shard, err := s.nodeMgr.GetShard(virtualStorage.Name)
		if err != nil {
			return nil, err
		}

		primary, err := shard.GetPrimary()
		if err != nil {
			return nil, err
		}

		secondaries, err := shard.GetSecondaries()
		if err != nil {
			return nil, err
		}

		nodes = append(append(nodes, primary), secondaries...)
	}
	var gitVersion, serverVersion string

	g, ctx := errgroup.WithContext(ctx)

	storageStatuses := make([][]*gitalypb.ServerInfoResponse_StorageStatus, len(nodes))

	for i, node := range nodes {
		i := i
		node := node

		g.Go(func() error {
			client := gitalypb.NewServerServiceClient(node.GetConnection())
			resp, err := client.ServerInfo(ctx, &gitalypb.ServerInfoRequest{})
			if err != nil {
				ctxlogrus.Extract(ctx).WithField("storage", node.GetStorage()).WithError(err).Error("error getting sever info")
				return nil
			}

			storageStatuses[i] = resp.GetStorageStatuses()

			once.Do(func() {
				gitVersion, serverVersion = resp.GetGitVersion(), resp.GetServerVersion()
			})

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

	response.GitVersion, response.ServerVersion = gitVersion, serverVersion

	return &response, nil
}
