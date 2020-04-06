package main

import (
	"errors"
	"flag"
	"fmt"
	"strconv"

	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
)

type sqlMigrateDownSubcommand struct {
	force bool
}

func (s *sqlMigrateDownSubcommand) FlagSet() *flag.FlagSet {
	flags := flag.NewFlagSet("sql-migrate-down", flag.ExitOnError)
	flags.Usage = func() {
		flag.PrintDefaults()
		printfErr("  MAX_MIGRATIONS\n")
		printfErr("\tNumber of migrations to roll back\n")
	}
	flags.BoolVar(&s.force, "f", false, "apply down-migrations (default is dry run)")
	return flags
}

func (s *sqlMigrateDownSubcommand) Exec(flags *flag.FlagSet, conf config.Config) error {
	if flags.NArg() != 1 {
		flags.Usage()
		return errors.New("invalid usage")
	}

	maxMigrations, err := strconv.Atoi(flags.Arg(0))
	if err != nil {
		return err
	}

	if maxMigrations < 1 {
		return fmt.Errorf("number of migrations to roll back must be 1 or more")
	}

	if s.force {
		n, err := datastore.MigrateDown(conf, maxMigrations)
		if err != nil {
			return err
		}

		fmt.Printf("OK (applied %d \"down\" migrations)\n", n)
		return nil
	}

	planned, err := datastore.MigrateDownPlan(conf, maxMigrations)
	if err != nil {
		return err
	}

	fmt.Printf("DRY RUN -- would roll back:\n\n")
	for _, id := range planned {
		fmt.Printf("- %s\n", id)
	}
	fmt.Printf("\nTo apply these migrations run with -f\n")

	return nil
}
