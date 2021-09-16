package glsql

import (
	"database/sql"
	"errors"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	migrate "github.com/rubenv/sql-migrate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
)

const (
	advisoryLockIDDatabaseTemplate = 1627644550
	praefectTemplateDatabase       = "praefect_template"
)

// TxWrapper is a simple wrapper around *sql.Tx.
type TxWrapper struct {
	*sql.Tx
}

// Rollback executes Rollback operation on the wrapped *sql.Tx if it is set.
// After execution is sets Tx to nil to prevent errors on the repeated invocations (useful
// for testing when Rollback is deferred).
func (txw *TxWrapper) Rollback(t testing.TB) {
	t.Helper()
	if txw.Tx != nil {
		require.NoError(t, txw.Tx.Rollback())
		txw.Tx = nil
	}
}

// Commit executes Commit operation on the wrapped *sql.Tx if it is set.
// After execution is sets Tx to nil to prevent errors on the deferred invocations (useful
// for testing when Rollback is deferred).
func (txw *TxWrapper) Commit(t testing.TB) {
	t.Helper()
	if txw.Tx != nil {
		require.NoError(t, txw.Tx.Commit())
		txw.Tx = nil
	}
}

// DB is a helper struct that should be used only for testing purposes.
type DB struct {
	*sql.DB
	// Name is a name of the database.
	Name string
}

// Begin starts a new transaction and returns it wrapped into TxWrapper.
func (db DB) Begin(t testing.TB) *TxWrapper {
	t.Helper()
	tx, err := db.DB.Begin()
	require.NoError(t, err)
	return &TxWrapper{Tx: tx}
}

// Truncate removes all data from the list of tables and restarts identities for them.
func (db DB) Truncate(t testing.TB, tables ...string) {
	t.Helper()

	for _, table := range tables {
		_, err := db.DB.Exec("DELETE FROM " + table)
		require.NoError(t, err, "database cleanup failed: %s", tables)
	}

	_, err := db.DB.Exec("SELECT setval(relname::TEXT, 1, false) from pg_class where relkind = 'S'")
	require.NoError(t, err, "database cleanup failed: %s", tables)
}

// RequireRowsInTable verifies that `tname` table has `n` amount of rows in it.
func (db DB) RequireRowsInTable(t *testing.T, tname string, n int) {
	t.Helper()

	var count int
	require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM "+tname).Scan(&count))
	require.Equal(t, n, count, "unexpected amount of rows in table: %d instead of %d", count, n)
}

// TruncateAll removes all data from known set of tables.
func (db DB) TruncateAll(t testing.TB) {
	db.Truncate(t,
		"replication_queue_job_lock",
		"replication_queue",
		"replication_queue_lock",
		"node_status",
		"shard_primaries",
		"storage_repositories",
		"repositories",
		"virtual_storages",
	)
}

// MustExec executes `q` with `args` and verifies there are no errors.
func (db DB) MustExec(t testing.TB, q string, args ...interface{}) {
	_, err := db.DB.Exec(q, args...)
	require.NoError(t, err)
}

// Close removes schema if it was used and releases connection pool.
func (db DB) Close() error {
	if err := db.DB.Close(); err != nil {
		return errors.New("failed to release connection pool: " + err.Error())
	}
	return nil
}

// NewDB returns a wrapper around the database connection pool.
// Must be used only for testing.
// The new database with empty relations will be created for each call of this function.
// It uses env vars:
//   PGHOST - required, URL/socket/dir
//   PGPORT - required, binding port
//   PGUSER - optional, user - `$ whoami` would be used if not provided
// Once the test is completed the database will be dropped on test cleanup execution.
func NewDB(t testing.TB) DB {
	t.Helper()
	database := "praefect_" + strings.ReplaceAll(uuid.New().String(), "-", "")
	return DB{DB: initPraefectTestDB(t, database), Name: database}
}

// GetDBConfig returns the database configuration determined by
// environment variables.  See NewDB() for the list of variables.
func GetDBConfig(t testing.TB, database string) config.DB {
	getEnvFromGDK(t)

	host, hostFound := os.LookupEnv("PGHOST")
	require.True(t, hostFound, "PGHOST env var expected to be provided to connect to Postgres database")

	port, portFound := os.LookupEnv("PGPORT")
	require.True(t, portFound, "PGPORT env var expected to be provided to connect to Postgres database")
	portNumber, pErr := strconv.Atoi(port)
	require.NoError(t, pErr, "PGPORT must be a port number of the Postgres database listens for incoming connections")

	// connect to 'postgres' database first to re-create testing database from scratch
	conf := config.DB{
		Host:    host,
		Port:    portNumber,
		DBName:  database,
		SSLMode: "disable",
		User:    os.Getenv("PGUSER"),
		SessionPooled: config.DBConnection{
			Host: host,
			Port: portNumber,
		},
	}

	bouncerHost, bouncerHostFound := os.LookupEnv("PGHOST_PGBOUNCER")
	if bouncerHostFound {
		conf.Host = bouncerHost
	}

	bouncerPort, bouncerPortFound := os.LookupEnv("PGPORT_PGBOUNCER")
	if bouncerPortFound {
		bouncerPortNumber, pErr := strconv.Atoi(bouncerPort)
		require.NoError(t, pErr, "PGPORT_PGBOUNCER must be a port number of the PgBouncer")

		conf.Port = bouncerPortNumber
	}

	return conf
}

func requireSQLOpen(t testing.TB, dbCfg config.DB, direct bool) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres", dbCfg.ToPQString(direct))
	require.NoErrorf(t, err, "failed to connect to %q database", dbCfg.DBName)
	if !assert.NoErrorf(t, db.Ping(), "failed to communicate with %q database", dbCfg.DBName) {
		require.NoErrorf(t, db.Close(), "release connection to the %q database", dbCfg.DBName)
	}
	return db
}

func requireTerminateAllConnections(t testing.TB, db *sql.DB, database string) {
	t.Helper()
	_, err := db.Exec("SELECT PG_TERMINATE_BACKEND(pid) FROM PG_STAT_ACTIVITY WHERE datname = '" + database + "'")
	require.NoError(t, err)
}

func initPraefectTestDB(t testing.TB, database string) *sql.DB {
	t.Helper()

	dbCfg := GetDBConfig(t, "postgres")
	// We require a direct connection to the Postgres instance and not through the PgBouncer
	// because we use transaction pool mood for it and it doesn't work well for system advisory locks.
	postgresDB := requireSQLOpen(t, dbCfg, true)
	defer func() { require.NoErrorf(t, postgresDB.Close(), "release connection to the %q database", dbCfg.DBName) }()

	// Acquire exclusive advisory lock to prevent other concurrent test from doing the same.
	_, err := postgresDB.Exec(`SELECT pg_advisory_lock($1)`, advisoryLockIDDatabaseTemplate)
	require.NoError(t, err, "not able to acquire lock for synchronisation")
	var advisoryUnlock func()
	advisoryUnlock = func() {
		require.True(t, scanSingleBool(t, postgresDB, `SELECT pg_advisory_unlock($1)`, advisoryLockIDDatabaseTemplate), "release advisory lock")
		advisoryUnlock = func() {}
	}
	defer func() { advisoryUnlock() }()

	templateDBExists := databaseExist(t, postgresDB, praefectTemplateDatabase)
	if !templateDBExists {
		_, err := postgresDB.Exec("CREATE DATABASE " + praefectTemplateDatabase + " WITH ENCODING 'UTF8'")
		require.NoErrorf(t, err, "failed to create %q database", praefectTemplateDatabase)
	}

	templateDBConf := GetDBConfig(t, praefectTemplateDatabase)
	templateDB := requireSQLOpen(t, templateDBConf, true)
	defer func() {
		require.NoErrorf(t, templateDB.Close(), "release connection to the %q database", templateDBConf.DBName)
	}()

	if _, err := Migrate(templateDB, false); err != nil {
		// If database has unknown migration we try to re-create template database with
		// current migration. It may be caused by other code changes done in another branch.
		if pErr := (*migrate.PlanError)(nil); errors.As(err, &pErr) {
			if strings.EqualFold(pErr.ErrorMessage, "unknown migration in database") {
				require.NoErrorf(t, templateDB.Close(), "release connection to the %q database", templateDBConf.DBName)

				_, err = postgresDB.Exec("DROP DATABASE " + praefectTemplateDatabase)
				require.NoErrorf(t, err, "failed to drop %q database", praefectTemplateDatabase)
				_, err = postgresDB.Exec("CREATE DATABASE " + praefectTemplateDatabase + " WITH ENCODING 'UTF8'")
				require.NoErrorf(t, err, "failed to create %q database", praefectTemplateDatabase)

				templateDB = requireSQLOpen(t, templateDBConf, true)
				defer func() {
					require.NoErrorf(t, templateDB.Close(), "release connection to the %q database", templateDBConf.DBName)
				}()
				_, err = Migrate(templateDB, false)
				require.NoErrorf(t, err, "failed to run database migration on %q", praefectTemplateDatabase)
			} else {
				require.NoErrorf(t, err, "failed to run database migration on %q", praefectTemplateDatabase)
			}
		} else {
			require.NoErrorf(t, err, "failed to run database migration on %q", praefectTemplateDatabase)
		}
	}

	// Release advisory lock as soon as possible to unblock other tests from execution.
	advisoryUnlock()

	require.NoErrorf(t, templateDB.Close(), "release connection to the %q database", templateDBConf.DBName)

	_, err = postgresDB.Exec(`CREATE DATABASE ` + database + ` TEMPLATE ` + praefectTemplateDatabase)
	require.NoErrorf(t, err, "failed to create %q database", praefectTemplateDatabase)

	t.Cleanup(func() {
		dbCfg.DBName = "postgres"
		postgresDB := requireSQLOpen(t, dbCfg, true)
		defer func() { require.NoErrorf(t, postgresDB.Close(), "release connection to the %q database", dbCfg.DBName) }()

		// We need to force-terminate open connections as for the tasks that use PgBouncer
		// the actual client connected to the database is a PgBouncer and not a test that is
		// running.
		requireTerminateAllConnections(t, postgresDB, database)

		_, err = postgresDB.Exec("DROP DATABASE " + database)
		require.NoErrorf(t, err, "failed to drop %q database", database)
	})

	// Connect to the testing database with optional PgBouncer
	dbCfg.DBName = database
	praefectTestDB := requireSQLOpen(t, dbCfg, false)
	t.Cleanup(func() {
		if err := praefectTestDB.Close(); !errors.Is(err, net.ErrClosed) {
			require.NoErrorf(t, err, "release connection to the %q database", dbCfg.DBName)
		}
	})
	return praefectTestDB
}

func databaseExist(t testing.TB, db *sql.DB, database string) bool {
	return scanSingleBool(t, db, `SELECT EXISTS(SELECT * FROM pg_database WHERE datname = $1)`, database)
}

func scanSingleBool(t testing.TB, db *sql.DB, query string, args ...interface{}) bool {
	var flag bool
	row := db.QueryRow(query, args...)
	require.NoError(t, row.Scan(&flag))
	return flag
}

func getEnvFromGDK(t testing.TB) {
	gdkEnv, err := exec.Command("gdk", "env").Output()
	if err != nil {
		// Assume we are not in a GDK setup; this is not an error so just return.
		return
	}

	for _, line := range strings.Split(string(gdkEnv), "\n") {
		const prefix = "export "
		if !strings.HasPrefix(line, prefix) {
			continue
		}

		split := strings.SplitN(strings.TrimPrefix(line, prefix), "=", 2)
		if len(split) != 2 {
			continue
		}
		key, value := split[0], split[1]

		require.NoError(t, os.Setenv(key, value), "set env var %v", key)
	}
}
