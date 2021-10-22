package praefect

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/migrations"
)

func TestPraefectMigrations_success(t *testing.T) {
	testCases := []struct {
		desc        string
		prepare     func(cfg config.Config) error
		expectedErr error
	}{
		{
			desc: "no migrations have run",
			prepare: func(cfg config.Config) error {
				_, err := datastore.MigrateDown(cfg, len(migrations.All()))
				if err != nil {
					return err
				}

				return nil
			},
			expectedErr: fmt.Errorf("%d migrations have not been run", len(migrations.All())),
		},
		{
			desc: "some migrations have run",
			prepare: func(cfg config.Config) error {
				_, err := datastore.MigrateDown(cfg, 3)
				if err != nil {
					return err
				}

				return nil
			},
			expectedErr: fmt.Errorf("3 migrations have not been run"),
		},
		{
			desc: "all migrations have run",
			prepare: func(cfg config.Config) error {
				return nil
			},
			expectedErr: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			var cfg config.Config
			db := glsql.NewDB(t)
			cfg.DB = glsql.GetDBConfig(t, db.Name)

			require.NoError(t, tc.prepare(cfg))

			migrationCheck := NewPraefectMigrationCheck(cfg)
			assert.Equal(t, "praefect migrations", migrationCheck.Name)
			assert.Equal(t, "confirms whether or not all praefect migrations have run", migrationCheck.Description)
			assert.Equal(t, tc.expectedErr, migrationCheck.Run())
		})
	}
}
