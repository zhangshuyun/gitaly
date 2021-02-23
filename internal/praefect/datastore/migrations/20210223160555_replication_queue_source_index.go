package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20210223160555_replication_queue_source_index",
		Up: []string{
			`CREATE INDEX CONCURRENTLY replication_queue_source_index
ON replication_queue (
	(job->>'virtual_storage'),
	(job->>'relative_path'),
	(job->>'source_node_storage')
)
WHERE state NOT IN ('completed', 'cancelled', 'dead')`,
		},
		Down:                 []string{"DROP INDEX replication_queue_source_index"},
		DisableTransactionUp: true,
	}

	allMigrations = append(allMigrations, m)
}
