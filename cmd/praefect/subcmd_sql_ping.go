package main

import (
	"flag"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
)

const (
	sqlPingCmdName = "sql-ping"
)

type sqlPingSubcommand struct{}

func (s *sqlPingSubcommand) FlagSet() *flag.FlagSet {
	return flag.NewFlagSet(sqlPingCmdName, flag.ExitOnError)
}

func (s *sqlPingSubcommand) Exec(flags *flag.FlagSet, conf config.Config) error {
	const subCmd = progname + " " + sqlPingCmdName

	db, clean, err := openDB(conf.DB)
	if err != nil {
		return err
	}
	defer clean()

	if err := datastore.CheckPostgresVersion(db); err != nil {
		return fmt.Errorf("%s: fail: %v", subCmd, err)
	}

	fmt.Printf("%s: OK\n", subCmd)
	return nil
}
