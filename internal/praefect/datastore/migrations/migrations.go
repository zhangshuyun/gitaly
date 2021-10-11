package migrations

import (
	migrate "github.com/rubenv/sql-migrate"
)

// MigrationTableName is the name of the SQL table used to store migration info.
const MigrationTableName = "schema_migrations"

var allMigrations []*migrate.Migration

// All returns all migrations defined in the package
func All() []*migrate.Migration {
	return allMigrations
}
