package main

import (
	"flag"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
)

const sqlMigrateCmdName = "sql-migrate"

type sqlMigrateSubcommand struct {
	ignoreUnknown bool
}

func (s *sqlMigrateSubcommand) FlagSet() *flag.FlagSet {
	flags := flag.NewFlagSet(sqlMigrateCmdName, flag.ExitOnError)
	flags.BoolVar(&s.ignoreUnknown, "ignore-unknown", true, "ignore unknown migrations (default is true)")
	return flags
}

func (s *sqlMigrateSubcommand) Exec(flags *flag.FlagSet, conf config.Config) error {
	const subCmd = progname + " " + sqlMigrateCmdName

	db, clean, err := openDB(conf.DB)
	if err != nil {
		return err
	}
	defer clean()

	n, err := glsql.Migrate(db, s.ignoreUnknown)
	if err != nil {
		return fmt.Errorf("%s: fail: %v", subCmd, err)
	}

	fmt.Printf("%s: OK (applied %d migrations)\n", subCmd, n)
	return nil
}
