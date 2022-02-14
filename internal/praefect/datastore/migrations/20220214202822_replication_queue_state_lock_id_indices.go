package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20220214202822_replication_queue_state_lock_id_indices",
		Up: []string{
			"CREATE INDEX CONCURRENTLY replication_queue_lock_id ON replication_queue (lock_id)",
			"CREATE INDEX CONCURRENTLY replication_queue_state ON replication_queue (state)",
		},
		DisableTransactionUp: true,
		Down: []string{
			"DROP INDEX replication_queue_state",
			"DROP INDEX replication_queue_lock_id",
		},
	}

	allMigrations = append(allMigrations, m)
}
