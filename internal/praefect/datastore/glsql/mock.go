package glsql

import (
	"context"
	"database/sql"
)

// MockQuerier allows for mocking database operations out.
type MockQuerier struct {
	ExecContextFunc func(context.Context, string, ...interface{}) (sql.Result, error)
	Querier
}

// ExecContext runs the mock's ExecContextFunc.
func (m MockQuerier) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return m.ExecContextFunc(ctx, query, args...)
}
