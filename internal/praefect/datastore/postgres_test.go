package datastore

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
)

func TestMigrateStatus(t *testing.T) {
	db := getDB(t)

	config := config.Config{
		DB: glsql.GetDBConfig(t, db.Name),
	}

	_, err := db.Exec("INSERT INTO schema_migrations VALUES ('2020_01_01_test', NOW())")
	require.NoError(t, err)

	rows, err := MigrateStatus(config)
	require.NoError(t, err)

	m := rows["20200109161404_hello_world"]
	require.True(t, m.Migrated)
	require.False(t, m.Unknown)

	m = rows["2020_01_01_test"]
	require.True(t, m.Migrated)
	require.True(t, m.Unknown)
}
