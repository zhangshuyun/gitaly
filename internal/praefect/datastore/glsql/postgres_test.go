package glsql_test

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testdb"
)

func TestOpenDB(t *testing.T) {
	dbCfg := testdb.GetConfig(t, "postgres")
	ctx, cancel := testhelper.Context()
	defer cancel()

	t.Run("failed to ping because of incorrect config", func(t *testing.T) {
		badCfg := dbCfg
		badCfg.Host = "not-existing.com"
		_, err := glsql.OpenDB(ctx, badCfg)
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

		ctx, cancel := testhelper.Context()
		cancel()

		_, err = glsql.OpenDB(ctx, badCfg)
		require.EqualError(t, err, "context canceled")
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
		{desc: "empty", in: config.DB{}, out: "binary_parameters=yes"},
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
			out:    `port=2345 host=1.2.3.4 user=praefect-user password=secret dbname=praefect_production sslmode=require sslcert=/path/to/cert sslkey=/path/to/key sslrootcert=/path/to/root-cert binary_parameters=yes`,
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
			out:    `port=2345 host=1.2.3.4 user=praefect-user password=secret dbname=praefect_production sslmode=require sslcert=/path/to/cert sslkey=/path/to/key sslrootcert=/path/to/root-cert binary_parameters=yes`,
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
			out:    `port=2345 host=1.2.3.4 user=praefect-user password=secret dbname=praefect_production_sp sslmode=require sslcert=/path/to/cert sslkey=/path/to/key sslrootcert=/path/to/root-cert binary_parameters=yes`,
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
			out:    `port=2345 host=1.2.3.4 user=praefect-user password=secret dbname=praefect_production sslmode=require sslcert=/path/to/cert sslkey=/path/to/key sslrootcert=/path/to/root-cert binary_parameters=yes`,
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
			out:    `port=6432 host=2.3.4.5 user=praefect_sp password=secret-sp dbname=praefect_production_sp sslmode=prefer sslcert=/path/to/sp/cert sslkey=/path/to/sp/key sslrootcert=/path/to/sp/root-cert binary_parameters=yes`,
		},
		{
			desc: "with spaces and quotes",
			in: config.DB{
				Password: "secret foo'bar",
			},
			out: `password=secret\ foo\'bar binary_parameters=yes`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.out, glsql.DSN(tc.in, tc.direct))
		})
	}
}
