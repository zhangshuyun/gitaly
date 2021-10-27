package glsql

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/migrations"
)

func TestOpenDB(t *testing.T) {
	dbCfg := GetDBConfig(t, "postgres")

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

func TestAdditionalTestMigrationsWereApplied(t *testing.T) {
	t.Parallel()

	const dbName = "migrations_verification"

	postgresDBCfg := GetDBConfig(t, "postgres")
	postgresDB := requireSQLOpen(t, postgresDBCfg, true)
	defer func() {
		require.NoErrorf(t, postgresDB.Close(), "release connection to the %q database", postgresDBCfg.DBName)
	}()

	_, err := postgresDB.Exec("DROP DATABASE IF EXISTS " + dbName + "")
	require.NoErrorf(t, err, `failed to drop %q database`, dbName)
	_, err = postgresDB.Exec("CREATE DATABASE " + dbName + " WITH ENCODING 'UTF8'")
	require.NoErrorf(t, err, `failed to create %q database`, dbName)

	migrationsDBCfg := GetDBConfig(t, dbName)
	migrationsDB := requireSQLOpen(t, migrationsDBCfg, true)
	defer func() {
		require.NoErrorf(t, migrationsDB.Close(), "release connection to the %q database", migrationsDBCfg.DBName)
	}()

	migrationsList := append(migrations.All(), testDataMigrations()...)
	_, err = Migrate(migrationsDB, false, migrationsList)
	require.NoError(t, err)

	var ms StringProvider
	rows, err := migrationsDB.Query(`SELECT id FROM ` + migrations.MigrationTableName)
	require.NoError(t, err)
	require.NoError(t, ScanAll(rows, &ms))
	require.Len(t, ms.Values(), len(migrationsList), "sanity check to verify all migrations were executed")
	var additionalMigrations []string
	for _, applied := range ms.Values() {
		var found bool
		for _, mig := range migrations.All() {
			if mig.Id == applied {
				found = true
				break
			}
		}
		if !found {
			additionalMigrations = append(additionalMigrations, applied)
		}
	}
	require.ElementsMatch(t, additionalMigrations, []string{
		"10000000000000_helpers",
		"20210906145020_artificial_repositories",
		"20210906145022_artificial_repositories_cleanup",
	}, "sanity check to verify all additional test migrations were applied")
}
