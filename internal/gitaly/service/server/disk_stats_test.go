package server

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"golang.org/x/sys/unix"
)

func TestStorageDiskStatistics(t *testing.T) {
	cfg := testcfg.Build(t)

	cfg.Storages = append(cfg.Storages, config.Storage{Name: "broken", Path: "/does/not/exist"})

	addr := runServer(t, cfg)
	client := newServerClient(t, addr)

	ctx, cancel := testhelper.Context()
	defer cancel()

	c, err := client.DiskStatistics(ctx, &gitalypb.DiskStatisticsRequest{})
	require.NoError(t, err)

	require.Len(t, c.GetStorageStatuses(), len(cfg.Storages))

	//used and available space may change so we check if it roughly matches (+/- 1GB)
	avail, used := getSpaceStats(t, cfg.Storages[0].Path)
	approxEqual(t, c.GetStorageStatuses()[0].Available, avail)
	approxEqual(t, c.GetStorageStatuses()[0].Used, used)
	require.Equal(t, cfg.Storages[0].Name, c.GetStorageStatuses()[0].StorageName)

	require.Equal(t, int64(0), c.GetStorageStatuses()[1].Available)
	require.Equal(t, int64(0), c.GetStorageStatuses()[1].Used)
	require.Equal(t, cfg.Storages[1].Name, c.GetStorageStatuses()[1].StorageName)
}

func approxEqual(t *testing.T, a, b int64) {
	const eps = 1024 * 1024 * 1024
	require.Truef(t, math.Abs(float64(a-b)) < eps, "expected %d to be equal %d with epsilon %d", a, b, eps)
}

func getSpaceStats(t *testing.T, path string) (available int64, used int64) {
	var stats unix.Statfs_t
	err := unix.Statfs(path, &stats)
	require.NoError(t, err)

	// Redundant conversions to handle differences between unix families
	available = int64(stats.Bavail) * int64(stats.Bsize)                   //nolint:unconvert,nolintlint
	used = (int64(stats.Blocks) - int64(stats.Bfree)) * int64(stats.Bsize) //nolint:unconvert,nolintlint
	return
}
