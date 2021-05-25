// +build postgres

package datastore

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
)

func TestMigrateStatus(t *testing.T) {
	db := getDB(t)

	config := config.Config{
		DB: getDBConfig(t),
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
