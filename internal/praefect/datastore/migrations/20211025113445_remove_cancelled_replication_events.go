package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20211025113445_remove_cancelled_replication_events",
		Up: []string{
			`DELETE FROM replication_queue WHERE state = 'cancelled'`,
		},
		Down: []string{
			// There is no way to restore deleted data.
		},
	}

	allMigrations = append(allMigrations, m)
}
