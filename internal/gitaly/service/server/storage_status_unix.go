// +build !openbsd

package server

import (
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"golang.org/x/sys/unix"
)

func getStorageStatus(shard config.Storage) (*gitalypb.DiskStatisticsResponse_StorageStatus, error) {
	var stats unix.Statfs_t
	err := unix.Statfs(shard.Path, &stats)
	if err != nil {
		return nil, err
	}

	// Redundant conversions to handle differences between unix families
	available := int64(stats.Bavail) * int64(stats.Bsize)                   //nolint:unconvert,nolintlint
	used := (int64(stats.Blocks) - int64(stats.Bfree)) * int64(stats.Bsize) //nolint:unconvert,nolintlint

	return &gitalypb.DiskStatisticsResponse_StorageStatus{
		StorageName: shard.Name,
		Available:   available,
		Used:        used,
	}, nil
}
