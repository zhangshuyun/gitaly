// Package glsql (Gitaly SQL) is a helper package to work with plain SQL queries.
package glsql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	// Blank import to enable integration of github.com/lib/pq into database/sql
	_ "github.com/lib/pq"
	migrate "github.com/rubenv/sql-migrate"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/migrations"
)

// OpenDB returns connection pool to the database.
func OpenDB(ctx context.Context, conf config.DB) (*sql.DB, error) {
	db, err := sql.Open("postgres", DSN(conf, false))
	if err != nil {
		return nil, err
	}

	errChan := make(chan error)
	go func() {
		if err := db.PingContext(ctx); err != nil {
			errChan <- fmt.Errorf("send ping: %w", err)
		} else {
			errChan <- nil
		}
	}()

	select {
	// Because of the issue https://github.com/lib/pq/issues/620 we need to handle context
	// cancellation/timeout by ourselves.
	case <-ctx.Done():
		db.Close()
		return nil, ctx.Err()
	case err := <-errChan:
		if err != nil {
			db.Close()
			return nil, err
		}
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
		{"binary_parameters", "yes"},
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
