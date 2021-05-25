// +build postgres

package glsql

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
)

func TestOpenDB(t *testing.T) {
	getEnvFromGDK(t)

	dbCfg := config.DB{
		Host: os.Getenv("PGHOST"),
		Port: func() int {
			pgPort := os.Getenv("PGPORT")
			port, err := strconv.Atoi(pgPort)
			require.NoError(t, err, "failed to parse PGPORT %q", pgPort)
			return port
		}(),
		DBName:  "postgres",
		SSLMode: "disable",
	}

	t.Run("failed to ping because of incorrect config", func(t *testing.T) {
		badCfg := dbCfg
		badCfg.Host = "not-existing.com"
		_, err := OpenDB(badCfg)
		require.Error(t, err, "opening of DB with incorrect configuration must fail")
	})

	t.Run("connected with proper config", func(t *testing.T) {
		db, err := OpenDB(dbCfg)
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
	db := getDB(t)

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
