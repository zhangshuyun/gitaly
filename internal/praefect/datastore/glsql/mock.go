package glsql

import (
	"context"
	"database/sql"
)

// MockQuerier allows for mocking database operations out.
type MockQuerier struct {
	ExecContextFunc  func(context.Context, string, ...interface{}) (sql.Result, error)
	QueryContextFunc func(context.Context, string, ...interface{}) (*sql.Rows, error)
	Querier
}

// ExecContext runs the mock's ExecContextFunc.
func (m MockQuerier) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return m.ExecContextFunc(ctx, query, args...)
}

// QueryContext runs the mock's QueryContextFunc.
func (m MockQuerier) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return m.QueryContextFunc(ctx, query, args...)
}
