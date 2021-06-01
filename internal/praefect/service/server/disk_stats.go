package server

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

// DiskStatistics sends DiskStatisticsRequest to all of a praefect server's internal gitaly nodes and aggregates the
// results into a response
func (s *Server) DiskStatistics(ctx context.Context, _ *gitalypb.DiskStatisticsRequest) (*gitalypb.DiskStatisticsResponse, error) {
	var storageStatuses [][]*gitalypb.DiskStatisticsResponse_StorageStatus

	for _, storages := range s.conns {
		for storage, conn := range storages {
			client := gitalypb.NewServerServiceClient(conn)
			resp, err := client.DiskStatistics(ctx, &gitalypb.DiskStatisticsRequest{})
			if err != nil {
				return nil, fmt.Errorf("error when requesting disk statistics from internal storage %v", storage)
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
