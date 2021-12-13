package testdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDB_Truncate(t *testing.T) {
	t.Parallel()
	db := New(t)

	_, err := db.Exec("CREATE TABLE truncate_tbl(id BIGSERIAL PRIMARY KEY)")
	require.NoError(t, err)

	res, err := db.Exec("INSERT INTO truncate_tbl VALUES (DEFAULT), (DEFAULT)")
	require.NoError(t, err)
	affected, err := res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(2), affected, "2 rows must be inserted into the table")

	db.Truncate(t, "truncate_tbl")

	var count int
	require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM truncate_tbl").Scan(&count))
	require.Equal(t, 0, count, "no rows must exist after TRUNCATE operation")

	var id int
	require.NoError(t, db.QueryRow("INSERT INTO truncate_tbl VALUES (DEFAULT) RETURNING id").Scan(&id))
	require.Equal(t, 1, id, "sequence for primary key must be restarted")
}
