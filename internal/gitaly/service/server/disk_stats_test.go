package server

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"golang.org/x/sys/unix"
)

func TestStorageDiskStatistics(t *testing.T) {
	// Setup storage paths
	testStorages := []config.Storage{
		{Name: "default", Path: testhelper.GitlabTestStoragePath()},
		{Name: "broken", Path: "/does/not/exist"},
	}
	defer func(oldStorages []config.Storage) {
		config.Config.Storages = oldStorages
	}(config.Config.Storages)
	config.Config.Storages = testStorages

	server, serverSocketPath := runServer(t, config.Config.Storages)
	defer server.Stop()

	client, conn := newServerClient(t, serverSocketPath)
	defer conn.Close() // nolint: errcheck

	ctx, cancel := testhelper.Context()
	defer cancel()

	c, err := client.DiskStatistics(ctx, &gitalypb.DiskStatisticsRequest{})
	require.NoError(t, err)

	require.Len(t, c.GetStorageStatuses(), len(testStorages))

	//used and available space may change so we check if it roughly matches (+/- 1GB)
	avail, used := getSpaceStats(t, testStorages[0].Path)
	approxEqual(t, c.GetStorageStatuses()[0].Available, avail)
	approxEqual(t, c.GetStorageStatuses()[0].Used, used)
	require.Equal(t, testStorages[0].Name, c.GetStorageStatuses()[0].StorageName)

	require.Equal(t, int64(0), c.GetStorageStatuses()[1].Available)
	require.Equal(t, int64(0), c.GetStorageStatuses()[1].Used)
	require.Equal(t, testStorages[1].Name, c.GetStorageStatuses()[1].StorageName)
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
	available = int64(stats.Bavail) * int64(stats.Bsize)                   //nolint:unconvert
	used = (int64(stats.Blocks) - int64(stats.Bfree)) * int64(stats.Bsize) //nolint:unconvert
	return
}
