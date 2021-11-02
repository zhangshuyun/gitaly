package glsql

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestOpenDB(t *testing.T) {
	dbCfg := GetDBConfig(t, "postgres")
	ctx, cancel := testhelper.Context()
	defer cancel()

	t.Run("failed to ping because of incorrect config", func(t *testing.T) {
		badCfg := dbCfg
		badCfg.Host = "not-existing.com"
		_, err := OpenDB(ctx, badCfg)
		require.Error(t, err)
		// Locally the error looks like:
		// 	send ping: dial tcp: lookup not-existing.com: no such host
		// but on CI it looks like:
		// 	send ping: dial tcp: lookup not-existing.com on 169.254.169.254:53: no such host
		// that is why regexp is used to check it.
		require.Regexp(t, "send ping: dial tcp: lookup not\\-existing.com(.*): no such host", err.Error(), "opening of DB with incorrect configuration must fail")
	})

	t.Run("timeout on hanging connection attempt", func(t *testing.T) {
		lis, err := net.Listen("tcp", ":0")
		require.NoError(t, err)
		badCfg := dbCfg
		badCfg.Host = "localhost"
		badCfg.Port = (lis.Addr().(*net.TCPAddr)).Port
		start := time.Now()
		ctx, cancel := context.WithTimeout(ctx, time.Nanosecond)
		defer cancel()
		_, err = OpenDB(ctx, badCfg)
		require.Equal(t, context.DeadlineExceeded, err, "context cancellation should prevent hang")
		duration := time.Since(start)
		require.Truef(t, duration < time.Second, "connection attempt took %s", duration.String())
	})

	t.Run("connected with proper config", func(t *testing.T) {
		db, err := OpenDB(ctx, dbCfg)
		require.NoError(t, err, "opening of DB with correct configuration must not fail")
		require.NoError(t, db.Close())
	})
}

func TestUint64Provider(t *testing.T) {
	var provider Uint64Provider

	dst1 := provider.To()
	require.Equal(t, []interface{}{new(uint64)}, dst1, "must be a single value holder")
	val1 := dst1[0].(*uint64)
	*val1 = uint64(100)

	dst2 := provider.To()
	require.Equal(t, []interface{}{new(uint64)}, dst2, "must be a single value holder")
	val2 := dst2[0].(*uint64)
	*val2 = uint64(200)

	require.Equal(t, []uint64{100, 200}, provider.Values())

	dst3 := provider.To()
	val3 := dst3[0].(*uint64)
	*val3 = uint64(300)

	require.Equal(t, []uint64{100, 200, 300}, provider.Values())
}

func TestScanAll(t *testing.T) {
	t.Parallel()
	db := NewDB(t)

	var ids Uint64Provider
	notEmptyRows, err := db.Query("SELECT id FROM (VALUES (1), (200), (300500)) AS t(id)")
	require.NoError(t, err)

	require.NoError(t, ScanAll(notEmptyRows, &ids))
	require.Equal(t, []uint64{1, 200, 300500}, ids.Values())

	var nothing Uint64Provider
	emptyRows, err := db.Query("SELECT id FROM (VALUES (1), (200), (300500)) AS t(id) WHERE id < 0")
	require.NoError(t, err)

	require.NoError(t, ScanAll(emptyRows, &nothing))
	require.Equal(t, ([]uint64)(nil), nothing.Values())
}
