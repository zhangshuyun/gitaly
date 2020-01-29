package server

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

// DiskStatistics sends DiskStatisticsRequest to all of a praefect server's internal gitaly nodes and aggregates the
// results into a response
func (s *Server) DiskStatistics(ctx context.Context, _ *gitalypb.DiskStatisticsRequest) (*gitalypb.DiskStatisticsResponse, error) {
	var storageStatuses [][]*gitalypb.DiskStatisticsResponse_StorageStatus

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

		for _, node := range append(secondaries, primary) {
			client := gitalypb.NewServerServiceClient(node.GetConnection())
			resp, err := client.DiskStatistics(ctx, &gitalypb.DiskStatisticsRequest{})
			if err != nil {
				return nil, fmt.Errorf("error when requesting disk statistics from internal storage %v", node.GetStorage())
			}

			storageStatuses = append(storageStatuses, resp.GetStorageStatuses())
		}
	}

	var response gitalypb.DiskStatisticsResponse

	for _, storageStatus := range storageStatuses {
		response.StorageStatuses = append(response.StorageStatuses, storageStatus...)
	}

	return &response, nil
}
