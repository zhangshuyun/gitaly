package server

import (
	"context"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func (s *server) DiskStatistics(ctx context.Context, _ *gitalypb.DiskStatisticsRequest) (*gitalypb.DiskStatisticsResponse, error) {
	var results []*gitalypb.DiskStatisticsResponse_StorageStatus
	for _, shard := range s.storages {
		shardInfo, err := getStorageStatus(shard)
		if err != nil {
			ctxlogrus.Extract(ctx).WithField("storage", shard).WithError(err).Error("to retrieve shard disk statistics")
			results = append(results, &gitalypb.DiskStatisticsResponse_StorageStatus{StorageName: shard.Name})
			continue
		}

		results = append(results, shardInfo)
	}

	return &gitalypb.DiskStatisticsResponse{
		StorageStatuses: results,
	}, nil
}
