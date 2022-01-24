// Package glsql (Gitaly SQL) is a helper package to work with plain SQL queries.
package glsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
	migrate "github.com/rubenv/sql-migrate"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/migrations"
)

// OpenDB returns connection pool to the database.
func OpenDB(ctx context.Context, conf config.DB) (*sql.DB, error) {
	connConfig, err := pgx.ParseConfig(DSN(conf, false))
	if err != nil {
		return nil, err
	}
	connStr := stdlib.RegisterConnConfig(connConfig)
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		return nil, err
	}

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("send ping: %w", err)
	}

	return db, nil
}

// DSN compiles configuration into data source name with lib/pq specifics.
func DSN(db config.DB, direct bool) string {
	var hostVal, userVal, passwordVal, dbNameVal string
	var sslModeVal, sslCertVal, sslKeyVal, sslRootCertVal string
	var portVal int

	if direct {
		hostVal = coalesceStr(db.SessionPooled.Host, db.HostNoProxy, db.Host)
		portVal = coalesceInt(db.SessionPooled.Port, db.PortNoProxy, db.Port)
		userVal = coalesceStr(db.SessionPooled.User, db.User)
		passwordVal = coalesceStr(db.SessionPooled.Password, db.Password)
		dbNameVal = coalesceStr(db.SessionPooled.DBName, db.DBName)
		sslModeVal = coalesceStr(db.SessionPooled.SSLMode, db.SSLMode)
		sslCertVal = coalesceStr(db.SessionPooled.SSLCert, db.SSLCert)
		sslKeyVal = coalesceStr(db.SessionPooled.SSLKey, db.SSLKey)
		sslRootCertVal = coalesceStr(db.SessionPooled.SSLRootCert, db.SSLRootCert)
	} else {
		hostVal = db.Host
		portVal = db.Port
		userVal = db.User
		passwordVal = db.Password
		dbNameVal = db.DBName
		sslModeVal = db.SSLMode
		sslCertVal = db.SSLCert
		sslKeyVal = db.SSLKey
		sslRootCertVal = db.SSLRootCert
	}

	var fields []string
	if portVal > 0 {
		fields = append(fields, fmt.Sprintf("port=%d", portVal))
	}

	for _, kv := range []struct{ key, value string }{
		{"host", hostVal},
		{"user", userVal},
		{"password", passwordVal},
		{"dbname", dbNameVal},
		{"sslmode", sslModeVal},
		{"sslcert", sslCertVal},
		{"sslkey", sslKeyVal},
		{"sslrootcert", sslRootCertVal},
		{"prefer_simple_protocol", "true"},
	} {
		if len(kv.value) == 0 {
			continue
		}

		kv.value = strings.ReplaceAll(kv.value, "'", `\'`)
		kv.value = strings.ReplaceAll(kv.value, " ", `\ `)

		fields = append(fields, kv.key+"="+kv.value)
	}

	return strings.Join(fields, " ")
}

// Migrate will apply all pending SQL migrations.
func Migrate(db *sql.DB, ignoreUnknown bool) (int, error) {
	migrationSet := migrate.MigrationSet{
		IgnoreUnknown: ignoreUnknown,
		TableName:     migrations.MigrationTableName,
	}

	migrationSource := &migrate.MemoryMigrationSource{
		Migrations: migrations.All(),
	}

	return migrationSet.Exec(db, "postgres", migrationSource, migrate.Up)
}

// MigrateSome will apply migration m and all unapplied migrations with earlier ids.
// To ensure a single migration is executed, run sql-migrate.PlanMigration and call
// MigrateSome for each migration returned.
func MigrateSome(m *migrate.Migration, db *sql.DB, ignoreUnknown bool) (int, error) {
	migrationSet := migrate.MigrationSet{
		IgnoreUnknown: ignoreUnknown,
		TableName:     migrations.MigrationTableName,
	}

	// sql-migrate.ToApply() expects all migrations prior to the final migration be present in the
	// in the slice. If we pass in only the target migration it will not be executed.
	migs := leadingMigrations(m)

	migrationSource := &migrate.MemoryMigrationSource{
		Migrations: migs,
	}

	return migrationSet.Exec(db, "postgres", migrationSource, migrate.Up)
}

// Create a slice of all migrations up to and including the one to be applied.
func leadingMigrations(target *migrate.Migration) []*migrate.Migration {
	allMigrations := migrations.All()

	for i, m := range allMigrations {
		if m.Id == target.Id {
			return allMigrations[:i+1]
		}
	}

	// Planned migration not found in migrations.All(), assume it is more recent
	// and return all migrations.
	return allMigrations
}

// Querier is an abstraction on *sql.DB and *sql.Tx that allows to use their methods without awareness about actual type.
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// Notification represent a notification from the database.
type Notification struct {
	// Channel is a name of the receiving channel.
	Channel string
	// Payload is a payload of the notification.
	Payload string
}

// ListenHandler contains a set of methods that would be called on corresponding notifications received.
type ListenHandler interface {
	// Notification would be triggered once a new notification received.
	Notification(Notification)
	// Disconnect would be triggered once a connection to remote service is lost.
	// Passed in error will never be nil and will contain cause of the disconnection.
	Disconnect(error)
	// Connected would be triggered once a connection to remote service is established.
	Connected()
}

// DestProvider returns list of pointers that will be used to scan values into.
type DestProvider interface {
	// To returns list of pointers.
	// It is not an idempotent operation and each call will return a new list.
	To() []interface{}
}

// ScanAll reads all data from 'rows' into holders provided by 'in'.
func ScanAll(rows *sql.Rows, in DestProvider) (err error) {
	for rows.Next() {
		if err = rows.Scan(in.To()...); err != nil {
			return err
		}
	}

	return nil
}

// Uint64Provider allows to use it with ScanAll function to read all rows into it and return result as a slice.
type Uint64Provider []*uint64

// Values returns list of values read from *sql.Rows
func (p *Uint64Provider) Values() []uint64 {
	if len(*p) == 0 {
		return nil
	}

	r := make([]uint64, len(*p))
	for i, v := range *p {
		r[i] = *v
	}
	return r
}

// To returns a list of pointers that will be used as a destination for scan operation.
func (p *Uint64Provider) To() []interface{} {
	var d uint64
	*p = append(*p, &d)
	return []interface{}{&d}
}

// StringProvider allows ScanAll to read all rows and return the result as a slice.
type StringProvider []*string

// Values returns list of values read from *sql.Rows
func (p *StringProvider) Values() []string {
	if len(*p) == 0 {
		return nil
	}

	r := make([]string, len(*p))
	for i, v := range *p {
		r[i] = *v
	}
	return r
}

// To returns a list of pointers that will be used as a destination for scan operation.
func (p *StringProvider) To() []interface{} {
	var d string
	*p = append(*p, &d)
	return []interface{}{&d}
}

func coalesceStr(values ...string) string {
	for _, cur := range values {
		if cur != "" {
			return cur
		}
	}
	return ""
}

func coalesceInt(values ...int) int {
	for _, cur := range values {
		if cur != 0 {
			return cur
		}
	}
	return 0
}

// StringArray is a wrapper that provides a helper methods.
type StringArray struct {
	pgtype.TextArray
}

// Slice converts StringArray into a slice of strings.
// The array element considered to be a valid string if it is not a null.
func (sa StringArray) Slice() []string {
	if sa.Status != pgtype.Present {
		return nil
	}

	res := make([]string, 0, len(sa.Elements))
	if sa.Status == pgtype.Present {
		for _, e := range sa.Elements {
			if e.Status != pgtype.Present {
				continue
			}
			res = append(res, e.String)
		}
	}
	return res
}

// errorCondition is a checker of the additional conditions of an error.
type errorCondition func(*pgconn.PgError) bool

// withConstraintName returns errorCondition that check if constraint name matches provided name.
func withConstraintName(name string) errorCondition {
	return func(pgErr *pgconn.PgError) bool {
		return pgErr.ConstraintName == name
	}
}

// IsQueryCancelled returns true if an error is a query cancellation.
func IsQueryCancelled(err error) bool {
	// https://www.postgresql.org/docs/11/errcodes-appendix.html
	// query_canceled
	return isPgError(err, "57014", nil)
}

// IsUniqueViolation returns true if an error is a unique violation.
func IsUniqueViolation(err error, constraint string) bool {
	// https://www.postgresql.org/docs/11/errcodes-appendix.html
	// unique_violation
	return isPgError(err, "23505", []errorCondition{withConstraintName(constraint)})
}

func isPgError(err error, code string, conditions []errorCondition) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == code {
		for _, condition := range conditions {
			if !condition(pgErr) {
				return false
			}
		}
		return true
	}
	return false
}
