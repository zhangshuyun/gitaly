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

func (*sqlMigrateDownSubcommand) prefix() string { return progname + " sql-migrate-down" }

func (*sqlMigrateDownSubcommand) invocation() string { return invocationPrefix + " sql-migrate-down" }

func (s *sqlMigrateDownSubcommand) FlagSet() *flag.FlagSet {
	flags := flag.NewFlagSet(s.prefix(), flag.ExitOnError)
	flags.Usage = func() {
		printfErr("usage:  %s [-f] MAX_MIGRATIONS\n", s.invocation())
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

		fmt.Printf("%s: OK (applied %d \"down\" migrations)\n", s.prefix(), n)
		return nil
	}

	planned, err := datastore.MigrateDownPlan(conf, maxMigrations)
	if err != nil {
		return err
	}

	fmt.Printf("%s: DRY RUN -- would roll back:\n\n", s.prefix())
	for _, id := range planned {
		fmt.Printf("- %s\n", id)
	}
	fmt.Printf("\nTo apply these migrations run: %s -f %d\n", s.invocation(), maxMigrations)

	return nil
}
