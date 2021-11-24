package datastore

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	migrate "github.com/rubenv/sql-migrate"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/migrations"
)

// MigrationStatusRow represents an entry in the schema migrations table.
// If the migration is in the database but is not listed, Unknown will be true.
type MigrationStatusRow struct {
	Migrated  bool
	Unknown   bool
	AppliedAt time.Time
}

// CheckPostgresVersion checks the server version of the Postgres DB
// specified in conf. This is a diagnostic for the Praefect Postgres
// rollout. https://gitlab.com/gitlab-org/gitaly/issues/1755
func CheckPostgresVersion(db *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var serverVersion int
	if err := db.QueryRowContext(ctx, "SHOW server_version_num").Scan(&serverVersion); err != nil {
		return fmt.Errorf("get postgres server version: %v", err)
	}

	// The minimum required Postgres server version is v11.0.
	if serverVersion < 11_00_00 {
		return fmt.Errorf("postgres server version too old: %d", serverVersion)
	}

	return nil
}

const sqlMigrateDialect = "postgres"

// MigrateDownPlan does a dry run for rolling back at most max migrations.
func MigrateDownPlan(conf config.Config, max int) ([]string, error) {
	ctx := context.Background()

	openDBCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	db, err := glsql.OpenDB(openDBCtx, conf.DB)
	if err != nil {
		return nil, fmt.Errorf("sql open: %v", err)
	}
	defer db.Close()

	migrationSet := migrate.MigrationSet{
		TableName: migrations.MigrationTableName,
	}

	planned, _, err := migrationSet.PlanMigration(db, sqlMigrateDialect, migrationSource(), migrate.Down, max)
	if err != nil {
		return nil, err
	}

	var result []string
	for _, m := range planned {
		result = append(result, m.Id)
	}

	return result, nil
}

// MigrateDown rolls back at most max migrations.
func MigrateDown(conf config.Config, max int) (int, error) {
	ctx := context.Background()

	openDBCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	db, err := glsql.OpenDB(openDBCtx, conf.DB)
	if err != nil {
		return 0, fmt.Errorf("sql open: %v", err)
	}
	defer db.Close()

	migrationSet := migrate.MigrationSet{
		TableName: migrations.MigrationTableName,
	}

	return migrationSet.ExecMax(db, sqlMigrateDialect, migrationSource(), migrate.Down, max)
}

// MigrateStatus returns the status of database migrations. The key of the map
// indexes the migration ID.
func MigrateStatus(conf config.Config) (map[string]*MigrationStatusRow, error) {
	ctx := context.Background()

	openDBCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	db, err := glsql.OpenDB(openDBCtx, conf.DB)
	if err != nil {
		return nil, fmt.Errorf("sql open: %v", err)
	}
	defer db.Close()

	migrationSet := migrate.MigrationSet{
		TableName: migrations.MigrationTableName,
	}

	migrations, err := migrationSource().FindMigrations()
	if err != nil {
		return nil, err
	}

	records, err := migrationSet.GetMigrationRecords(db, sqlMigrateDialect)
	if err != nil {
		return nil, err
	}

	rows := make(map[string]*MigrationStatusRow)

	for _, m := range migrations {
		rows[m.Id] = &MigrationStatusRow{
			Migrated: false,
		}
	}

	for _, r := range records {
		if rows[r.Id] == nil {
			rows[r.Id] = &MigrationStatusRow{
				Unknown: true,
			}
		}

		rows[r.Id].Migrated = true
		rows[r.Id].AppliedAt = r.AppliedAt
	}

	return rows, nil
}

func migrationSource() *migrate.MemoryMigrationSource {
	return &migrate.MemoryMigrationSource{Migrations: migrations.All()}
}
