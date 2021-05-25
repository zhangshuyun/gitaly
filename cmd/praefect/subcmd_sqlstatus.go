package main

import (
	"flag"
	"os"
	"sort"

	"github.com/olekukonko/tablewriter"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
)

type sqlMigrateStatusSubcommand struct{}

func (s *sqlMigrateStatusSubcommand) FlagSet() *flag.FlagSet {
	return flag.NewFlagSet("sql-migrate-status", flag.ExitOnError)
}

func (s *sqlMigrateStatusSubcommand) Exec(flags *flag.FlagSet, conf config.Config) error {
	migrations, err := datastore.MigrateStatus(conf)
	if err != nil {
		return err
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Migration", "Applied"})
	table.SetColWidth(60)

	// Display the rows in order of name
	var keys []string
	for k := range migrations {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		m := migrations[k]
		applied := "no"

		if m.Unknown {
			applied = "unknown migration"
		} else if m.Migrated {
			applied = m.AppliedAt.String()
		}

		table.Append([]string{
			k,
			applied,
		})
	}

	table.Render()

	return err
}
