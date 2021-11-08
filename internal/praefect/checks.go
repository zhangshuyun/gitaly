package praefect

import (
	"context"
	"fmt"

	migrate "github.com/rubenv/sql-migrate"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/migrations"
)

// Severity is a type that indicates the severity of a check
type Severity string

const (
	// Warning indicates a severity level of warning
	Warning Severity = "warning"
	// Fatal indicates a severity level of fatal
	// any checks that are Fatal will prevent Praefect from starting up
	Fatal = "fatal"
)

// Check is a struct representing a check on the health of a Gitaly cluster's setup. These are separate from the "healthcheck"
// concept which is more concerned with the health of the praefect service. These checks are meant to diagnose any issues with
// the praefect cluster setup itself and will be run on startup/restarts.
type Check struct {
	Run         func(ctx context.Context) error
	Name        string
	Description string
	Severity    Severity
}

// CheckFunc is a function type that takes a praefect config and returns a Check
type CheckFunc func(conf config.Config) *Check

// NewPraefectMigrationCheck returns a Check that checks if all praefect migrations have run
func NewPraefectMigrationCheck(conf config.Config) *Check {
	return &Check{
		Name:        "praefect migrations",
		Description: "confirms whether or not all praefect migrations have run",
		Run: func(ctx context.Context) error {
			db, err := glsql.OpenDB(conf.DB)
			if err != nil {
				return err
			}
			defer db.Close()

			migrationSet := migrate.MigrationSet{
				TableName: migrations.MigrationTableName,
			}

			migrationSource := &migrate.MemoryMigrationSource{
				Migrations: migrations.All(),
			}

			migrationsNeeded, _, err := migrationSet.PlanMigration(db, "postgres", migrationSource, migrate.Up, 0)
			if err != nil {
				return err
			}

			missingMigrations := len(migrationsNeeded)

			if missingMigrations > 0 {
				return fmt.Errorf("%d migrations have not been run", missingMigrations)
			}

			return nil
		},
		Severity: Fatal,
	}
}
