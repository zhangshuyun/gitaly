package glsql_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	migrate "github.com/rubenv/sql-migrate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/migrations"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testdb"
)

func TestOpenDB(t *testing.T) {
	dbCfg := testdb.GetConfig(t, "postgres")
	ctx := testhelper.Context(t)

	t.Run("failed to ping because of incorrect config", func(t *testing.T) {
		badCfg := dbCfg
		badCfg.Host = "not-existing.com"
		_, err := glsql.OpenDB(ctx, badCfg)
		require.Error(t, err)
		// The regexp is used because error message has a diff in local run an on CI.
		const errRegexp = "send ping: failed to connect to `host=not\\-existing.com user=.* database=.*`: hostname resolving error"
		require.Regexp(t, errRegexp, err.Error(), "opening of DB with incorrect configuration must fail")
	})

	t.Run("timeout on hanging connection attempt", func(t *testing.T) {
		lis, err := net.Listen("tcp", "localhost:0")
		require.NoError(t, err)
		badCfg := dbCfg
		badCfg.Host = "localhost"
		badCfg.Port = (lis.Addr().(*net.TCPAddr)).Port
		start := time.Now()

		ctx, cancel := context.WithCancel(testhelper.Context(t))
		cancel()

		_, err = glsql.OpenDB(ctx, badCfg)
		require.EqualError(t, err, "send ping: context canceled")
		duration := time.Since(start)
		require.Truef(t, duration < time.Second, "connection attempt took %s", duration.String())
	})

	t.Run("connected with proper config", func(t *testing.T) {
		db, err := glsql.OpenDB(ctx, dbCfg)
		require.NoError(t, err, "opening of DB with correct configuration must not fail")
		require.NoError(t, db.Close())
	})
}

func TestUint64Provider(t *testing.T) {
	var provider glsql.Uint64Provider

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
	db := testdb.New(t)

	var ids glsql.Uint64Provider
	notEmptyRows, err := db.Query("SELECT id FROM (VALUES (1), (200), (300500)) AS t(id)")
	require.NoError(t, err)
	defer func() { require.NoError(t, notEmptyRows.Close()) }()

	require.NoError(t, glsql.ScanAll(notEmptyRows, &ids))
	require.Equal(t, []uint64{1, 200, 300500}, ids.Values())
	require.NoError(t, notEmptyRows.Err())

	var nothing glsql.Uint64Provider
	emptyRows, err := db.Query("SELECT id FROM (VALUES (1), (200), (300500)) AS t(id) WHERE id < 0")
	require.NoError(t, err)
	defer func() { require.NoError(t, emptyRows.Close()) }()

	require.NoError(t, glsql.ScanAll(emptyRows, &nothing))
	require.Equal(t, ([]uint64)(nil), nothing.Values())
	require.NoError(t, emptyRows.Err())
}

func TestDSN(t *testing.T) {
	testCases := []struct {
		desc   string
		in     config.DB
		direct bool
		out    string
	}{
		{desc: "empty", in: config.DB{}, out: "prefer_simple_protocol=true"},
		{
			desc: "proxy connection",
			in: config.DB{
				Host:        "1.2.3.4",
				Port:        2345,
				User:        "praefect-user",
				Password:    "secret",
				DBName:      "praefect_production",
				SSLMode:     "require",
				SSLCert:     "/path/to/cert",
				SSLKey:      "/path/to/key",
				SSLRootCert: "/path/to/root-cert",
			},
			direct: false,
			out:    `port=2345 host=1.2.3.4 user=praefect-user password=secret dbname=praefect_production sslmode=require sslcert=/path/to/cert sslkey=/path/to/key sslrootcert=/path/to/root-cert prefer_simple_protocol=true`,
		},
		{
			desc: "direct connection with different host and port",
			in: config.DB{
				User:        "praefect-user",
				Password:    "secret",
				DBName:      "praefect_production",
				SSLMode:     "require",
				SSLCert:     "/path/to/cert",
				SSLKey:      "/path/to/key",
				SSLRootCert: "/path/to/root-cert",
				SessionPooled: config.DBConnection{
					Host: "1.2.3.4",
					Port: 2345,
				},
			},
			direct: true,
			out:    `port=2345 host=1.2.3.4 user=praefect-user password=secret dbname=praefect_production sslmode=require sslcert=/path/to/cert sslkey=/path/to/key sslrootcert=/path/to/root-cert prefer_simple_protocol=true`,
		},
		{
			desc: "direct connection with dbname",
			in: config.DB{
				Host:        "1.2.3.4",
				Port:        2345,
				User:        "praefect-user",
				Password:    "secret",
				DBName:      "praefect_production",
				SSLMode:     "require",
				SSLCert:     "/path/to/cert",
				SSLKey:      "/path/to/key",
				SSLRootCert: "/path/to/root-cert",
				SessionPooled: config.DBConnection{
					DBName: "praefect_production_sp",
				},
			},
			direct: true,
			out:    `port=2345 host=1.2.3.4 user=praefect-user password=secret dbname=praefect_production_sp sslmode=require sslcert=/path/to/cert sslkey=/path/to/key sslrootcert=/path/to/root-cert prefer_simple_protocol=true`,
		},
		{
			desc: "direct connection with exactly the same parameters",
			in: config.DB{
				Host:          "1.2.3.4",
				Port:          2345,
				User:          "praefect-user",
				Password:      "secret",
				DBName:        "praefect_production",
				SSLMode:       "require",
				SSLCert:       "/path/to/cert",
				SSLKey:        "/path/to/key",
				SSLRootCert:   "/path/to/root-cert",
				SessionPooled: config.DBConnection{},
			},
			direct: true,
			out:    `port=2345 host=1.2.3.4 user=praefect-user password=secret dbname=praefect_production sslmode=require sslcert=/path/to/cert sslkey=/path/to/key sslrootcert=/path/to/root-cert prefer_simple_protocol=true`,
		},
		{
			desc: "direct connection with completely different parameters",
			in: config.DB{
				Host:        "1.2.3.4",
				Port:        2345,
				User:        "praefect-user",
				Password:    "secret",
				DBName:      "praefect_production",
				SSLMode:     "require",
				SSLCert:     "/path/to/cert",
				SSLKey:      "/path/to/key",
				SSLRootCert: "/path/to/root-cert",
				SessionPooled: config.DBConnection{
					Host:        "2.3.4.5",
					Port:        6432,
					User:        "praefect_sp",
					Password:    "secret-sp",
					DBName:      "praefect_production_sp",
					SSLMode:     "prefer",
					SSLCert:     "/path/to/sp/cert",
					SSLKey:      "/path/to/sp/key",
					SSLRootCert: "/path/to/sp/root-cert",
				},
			},
			direct: true,
			out:    `port=6432 host=2.3.4.5 user=praefect_sp password=secret-sp dbname=praefect_production_sp sslmode=prefer sslcert=/path/to/sp/cert sslkey=/path/to/sp/key sslrootcert=/path/to/sp/root-cert prefer_simple_protocol=true`,
		},
		{
			desc: "with spaces and quotes",
			in: config.DB{
				Password: "secret foo'bar",
			},
			out: `password=secret\ foo\'bar prefer_simple_protocol=true`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.out, glsql.DSN(tc.in, tc.direct))
		})
	}
}

func TestStringArray(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)

	t.Run("multiple elements", func(t *testing.T) {
		var res glsql.StringArray
		require.NoError(t, db.QueryRow("SELECT ARRAY['a', NULL, '42', 'c']").Scan(&res))
		require.Equal(t, []string{"a", "42", "c"}, res.Slice())
	})

	t.Run("NULL value", func(t *testing.T) {
		var res glsql.StringArray
		require.NoError(t, db.QueryRow("SELECT NULL").Scan(&res))
		require.Empty(t, res.Slice())
	})
}

func TestIsQueryCancelled(t *testing.T) {
	for _, tc := range []struct {
		desc string
		err  error
		exp  bool
	}{
		{
			desc: "nil input",
			err:  nil,
			exp:  false,
		},
		{
			desc: "wrong error type",
			err:  assert.AnError,
			exp:  false,
		},
		{
			desc: "wrong code",
			err:  &pgconn.PgError{Code: "stub"},
			exp:  false,
		},
		{
			desc: "cancellation error",
			err:  &pgconn.PgError{Code: "57014"},
			exp:  true,
		},
		{
			desc: "wrapped cancellation error",
			err:  fmt.Errorf("stub: %w", &pgconn.PgError{Code: "57014"}),
			exp:  true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			res := glsql.IsQueryCancelled(tc.err)
			require.Equal(t, tc.exp, res)
		})
	}
}

func TestIsUniqueViolation(t *testing.T) {
	for _, tc := range []struct {
		desc   string
		err    error
		constr string
		exp    bool
	}{
		{
			desc: "nil input",
			err:  nil,
			exp:  false,
		},
		{
			desc: "wrong error type",
			err:  assert.AnError,
			exp:  false,
		},
		{
			desc: "wrong code",
			err:  &pgconn.PgError{Code: "stub"},
			exp:  false,
		},
		{
			desc: "unique violation",
			err:  &pgconn.PgError{Code: "23505"},
			exp:  true,
		},
		{
			desc: "wrapped unique violation",
			err:  fmt.Errorf("stub: %w", &pgconn.PgError{Code: "23505"}),
			exp:  true,
		},
		{
			desc:   "unique violation with accepted conditions",
			err:    &pgconn.PgError{Code: "23505", ConstraintName: "cname"},
			constr: "cname",
			exp:    true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			res := glsql.IsUniqueViolation(tc.err, tc.constr)
			require.Equal(t, tc.exp, res)
		})
	}
}

func TestMigrateSome(t *testing.T) {
	db := testdb.New(t)
	dbCfg := testdb.GetConfig(t, db.Name)
	cfg := config.Config{DB: dbCfg}

	migs := migrations.All()
	migrationCt := len(migs)

	for _, tc := range []struct {
		desc      string
		up        int
		executed  int
		migration *migrate.Migration
	}{
		{
			desc:      "All migrations up",
			up:        migrationCt,
			executed:  0,
			migration: migs[migrationCt-1],
		},
		{
			desc:      "Apply only first migration",
			up:        0,
			executed:  1,
			migration: migs[0],
		},
		{
			desc:      "Apply only last migration",
			up:        migrationCt - 1,
			executed:  1,
			migration: migs[migrationCt-1],
		},
		{
			desc:      "Apply only 10th migration",
			up:        9,
			executed:  1,
			migration: migs[9],
		},
		{
			desc:      "Apply 5th to 10th migrations",
			up:        5,
			executed:  5,
			migration: migs[9],
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			testdb.SetMigrations(t, db, cfg, tc.up)

			n, err := glsql.MigrateSome(tc.migration, db.DB, true)
			assert.NoError(t, err)
			assert.Equal(t, tc.executed, n)
		})
	}
}
