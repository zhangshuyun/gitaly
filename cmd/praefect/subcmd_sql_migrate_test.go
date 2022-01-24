package main

import (
	"bytes"
	"flag"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/migrations"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testdb"
)

func TestSubCmdSqlMigrate(t *testing.T) {
	db := testdb.New(t)
	dbCfg := testdb.GetConfig(t, db.Name)
	cfg := config.Config{DB: dbCfg}

	migrationCt := len(migrations.All())

	for _, tc := range []struct {
		desc           string
		up             int
		expectedOutput []string
	}{
		{
			desc:           "All migrations up",
			up:             migrationCt,
			expectedOutput: []string{"praefect sql-migrate: all migrations are up"},
		},
		{
			desc: "All migrations down",
			up:   0,
			expectedOutput: []string{
				fmt.Sprintf("praefect sql-migrate: migrations to apply: %d", migrationCt),
				"20200109161404_hello_world: migrating",
				"20200109161404_hello_world: applied (",
				fmt.Sprintf("praefect sql-migrate: OK (applied %d migrations)", migrationCt),
			},
		},
		{
			desc: "Some migrations down",
			up:   10,
			expectedOutput: []string{
				fmt.Sprintf("praefect sql-migrate: migrations to apply: %d", migrationCt-10),
				"20201126165633_repository_assignments_table: migrating",
				"20201126165633_repository_assignments_table: applied (",
				fmt.Sprintf("praefect sql-migrate: OK (applied %d migrations)", migrationCt-10),
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			testdb.SetMigrations(t, db, cfg, tc.up)

			var stdout bytes.Buffer
			migrateCmd := sqlMigrateSubcommand{w: &stdout, ignoreUnknown: true}
			assert.NoError(t, migrateCmd.Exec(flag.NewFlagSet("", flag.PanicOnError), cfg))

			for _, out := range tc.expectedOutput {
				assert.Contains(t, stdout.String(), out)
			}
		})
	}
}
