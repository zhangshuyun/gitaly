package main

import (
	"flag"
	"fmt"
	"io"
	"time"

	migrate "github.com/rubenv/sql-migrate"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/migrations"
)

const (
	sqlMigrateCmdName = "sql-migrate"
	timeFmt           = "2006-01-02T15:04:05"
)

type sqlMigrateSubcommand struct {
	w             io.Writer
	ignoreUnknown bool
}

func newSQLMigrateSubCommand(writer io.Writer) *sqlMigrateSubcommand {
	return &sqlMigrateSubcommand{w: writer}
}

func (cmd *sqlMigrateSubcommand) FlagSet() *flag.FlagSet {
	flags := flag.NewFlagSet(sqlMigrateCmdName, flag.ExitOnError)
	flags.BoolVar(&cmd.ignoreUnknown, "ignore-unknown", true, "ignore unknown migrations (default is true)")
	return flags
}

func (cmd *sqlMigrateSubcommand) Exec(flags *flag.FlagSet, conf config.Config) error {
	const subCmd = progname + " " + sqlMigrateCmdName

	db, clean, err := openDB(conf.DB)
	if err != nil {
		return err
	}
	defer clean()

	migrationSet := migrate.MigrationSet{
		IgnoreUnknown: cmd.ignoreUnknown,
		TableName:     migrations.MigrationTableName,
	}

	planSource := &migrate.MemoryMigrationSource{
		Migrations: migrations.All(),
	}

	// Find all migrations that are currently down.
	planMigrations, _, _ := migrationSet.PlanMigration(db, "postgres", planSource, migrate.Up, 0)

	if len(planMigrations) == 0 {
		fmt.Fprintf(cmd.w, "%s: all migrations are up\n", subCmd)
		return nil
	}
	fmt.Fprintf(cmd.w, "%s: migrations to apply: %d\n\n", subCmd, len(planMigrations))

	executed := 0
	for _, mig := range planMigrations {
		fmt.Fprintf(cmd.w, "=  %s %v: migrating\n", time.Now().Format(timeFmt), mig.Id)
		start := time.Now()

		n, err := glsql.MigrateSome(mig.Migration, db, cmd.ignoreUnknown)
		if err != nil {
			return fmt.Errorf("%s: fail: %v", time.Now().Format(timeFmt), err)
		}

		if n > 0 {
			fmt.Fprintf(cmd.w, "== %s %v: applied (%s)\n", time.Now().Format(timeFmt), mig.Id, time.Since(start))

			// Additional migrations were run. No harm, but prevents us from tracking their execution duration.
			if n > 1 {
				fmt.Fprintf(cmd.w, "warning: %v additional migrations were applied successfully\n", n-1)
			}
		} else {
			fmt.Fprintf(cmd.w, "== %s %v: skipped (%s)\n", time.Now().Format(timeFmt), mig.Id, time.Since(start))
		}

		executed += n
	}

	fmt.Fprintf(cmd.w, "\n%s: OK (applied %d migrations)\n", subCmd, executed)
	return nil
}
