// +build postgres

package glsql

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
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

func TestTxQuery_MultipleOperationsSuccess(t *testing.T) {
	db := getDB(t)
	defer createBasicTable(t, db, "work_unit_test")()

	ctx, cancel := testhelper.Context()
	defer cancel()

	const actions = 3
	txq := NewTxQuery(context.TODO(), nil, db.DB)

	defer func() {
		var err error
		txq.Done(&err)
		require.NoError(t, err)

		db.RequireRowsInTable(t, "work_unit_test", actions)
	}()

	for i := 0; i < actions; i++ {
		require.True(
			t,
			txq.Exec(ctx, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, "INSERT INTO work_unit_test VALUES (DEFAULT)")
				return err
			}),
			"expects row to be inserted",
		)
	}
}

func TestTxQuery_FailedOperationInTheMiddle(t *testing.T) {
	db := getDB(t)
	defer createBasicTable(t, db, "work_unit_test")()

	ctx, cancel := testhelper.Context()
	defer cancel()

	txq := NewTxQuery(ctx, nil, db.DB)

	defer func() {
		var err error
		txq.Done(&err)
		require.EqualError(t, err, `pq: syntax error at or near "BAD"`, "expects error because of the incorrect SQL statement")

		db.RequireRowsInTable(t, "work_unit_test", 0)
	}()

	require.True(t,
		txq.Exec(ctx, func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, "INSERT INTO work_unit_test(id) VALUES (DEFAULT)")
			return err
		}),
		"expects row to be inserted",
	)

	require.False(t,
		txq.Exec(ctx, func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, "BAD OPERATION")
			return err
		}),
		"the SQL statement is not valid, expects to be reported as failed",
	)

	require.False(t,
		txq.Exec(ctx, func(ctx context.Context, tx *sql.Tx) error {
			t.Fatal("this func should not be called")
			return nil
		}),
		"because of previously failed SQL operation next statement expected not to be run",
	)
}

func TestTxQuery_ContextHandled(t *testing.T) {
	db := getDB(t)

	defer createBasicTable(t, db, "work_unit_test")()

	ctx, cancel := testhelper.Context()
	defer cancel()

	txq := NewTxQuery(ctx, nil, db.DB)

	defer func() {
		var err error
		txq.Done(&err)
		require.EqualError(t, err, "context canceled")

		db.RequireRowsInTable(t, "work_unit_test", 0)
	}()

	require.True(t,
		txq.Exec(ctx, func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, "INSERT INTO work_unit_test(id) VALUES (DEFAULT)")
			return err
		}),
		"expects row to be inserted",
	)

	cancel() // explicit context cancellation to simulate situation when it is expired or cancelled

	require.False(t,
		txq.Exec(ctx, func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, "INSERT INTO work_unit_test(id) VALUES (DEFAULT)")
			return err
		}),
		"expects failed operation because of cancelled context",
	)
}

func TestTxQuery_FailedToCommit(t *testing.T) {
	db := getDB(t)
	defer createBasicTable(t, db, "work_unit_test")()

	ctx, cancel := testhelper.Context()
	defer cancel()

	txq := NewTxQuery(ctx, nil, db.DB)

	defer func() {
		var err error
		txq.Done(&err)
		require.EqualError(t, err, sql.ErrTxDone.Error(), "expects failed COMMIT because of previously executed COMMIT statement")

		db.RequireRowsInTable(t, "work_unit_test", 1)
	}()

	require.True(t,
		txq.Exec(ctx, func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, "INSERT INTO work_unit_test(id) VALUES (DEFAULT)")
			return err
		}),
		"expects row to be inserted",
	)

	require.True(t,
		txq.Exec(ctx, func(ctx context.Context, tx *sql.Tx) error {
			require.NoError(t, tx.Commit()) // COMMIT to get error on the next attempt to COMMIT from Done method
			return nil
		}),
		"expects COMMIT without issues",
	)
}

func TestTxQuery_FailedToRollbackWithFailedOperation(t *testing.T) {
	db := getDB(t)
	defer createBasicTable(t, db, "work_unit_test")()

	ctx, cancel := testhelper.Context()
	defer cancel()

	outBuffer := &bytes.Buffer{}
	logger := logrus.New()
	logger.Out = outBuffer
	logger.Level = logrus.ErrorLevel
	logger.Formatter = &logrus.JSONFormatter{
		DisableTimestamp: true,
		PrettyPrint:      false,
	}

	txq := NewTxQuery(ctx, logger, db.DB)

	defer func() {
		var err error
		txq.Done(&err)
		require.EqualError(t, err, "some unexpected error")
		require.Equal(t,
			`{"error":"sql: transaction has already been committed or rolled back","level":"error","msg":"rollback failed"}`,
			strings.TrimSpace(outBuffer.String()),
			"failed COMMIT/ROLLBACK operation must be logged in case of another error during transaction usage",
		)

		db.RequireRowsInTable(t, "work_unit_test", 1)
	}()

	require.True(t,
		txq.Exec(ctx, func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, "INSERT INTO work_unit_test(id) VALUES (DEFAULT)")
			return err
		}),
		"expects row to be inserted",
	)

	require.False(t,
		txq.Exec(ctx, func(ctx context.Context, tx *sql.Tx) error {
			require.NoError(t, tx.Commit(), "expects successful COMMIT") // COMMIT to get error on the next attempt to COMMIT
			return errors.New("some unexpected error")
		}),
		"expects failed operation because of explicit error returned",
	)
}

func TestTxQuery_FailedToCommitWithFailedOperation(t *testing.T) {
	db := getDB(t)
	defer createBasicTable(t, db, "work_unit_test")()

	ctx, cancel := testhelper.Context()
	defer cancel()

	outBuffer := &bytes.Buffer{}
	logger := logrus.New()
	logger.Out = outBuffer
	logger.Level = logrus.ErrorLevel
	logger.Formatter = &logrus.JSONFormatter{
		DisableTimestamp: true,
		PrettyPrint:      false,
	}

	txq := NewTxQuery(ctx, logger, db.DB)

	defer func() {
		err := errors.New("some processing error")
		txq.Done(&err)
		require.EqualError(t, err, "some processing error")
		require.Equal(
			t,
			`{"error":"sql: transaction has already been committed or rolled back","level":"error","msg":"commit failed"}`,
			strings.TrimSpace(outBuffer.String()),
			"failed COMMIT/ROLLBACK operation must be logged in case of another error during transaction usage",
		)

		db.RequireRowsInTable(t, "work_unit_test", 1)
	}()

	require.True(t,
		txq.Exec(ctx, func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, "INSERT INTO work_unit_test(id) VALUES (DEFAULT)")
			return err
		}),
		"expects row to be inserted",
	)

	require.True(t,
		txq.Exec(ctx, func(ctx context.Context, tx *sql.Tx) error {
			require.NoError(t, tx.Commit()) // COMMIT to get error on the next attempt to COMMIT
			return nil
		}),
		"expects COMMIT without issues",
	)
}

func createBasicTable(t *testing.T, db DB, tname string) func() {
	t.Helper()

	_, err := db.Exec("CREATE TABLE " + tname + "(id BIGSERIAL PRIMARY KEY, col TEXT)")
	require.NoError(t, err)
	return func() {
		_, err := db.Exec("DROP TABLE IF EXISTS " + tname)
		require.NoError(t, err)
	}
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
