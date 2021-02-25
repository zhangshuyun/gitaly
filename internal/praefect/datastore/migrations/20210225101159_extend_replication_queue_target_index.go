package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20210225101159_replication_queue_target_index",
		Up: []string{
			`
CREATE INDEX CONCURRENTLY replication_queue_target_index
ON replication_queue (
	(job->>'virtual_storage'),
	(job->>'relative_path'),
	(job->>'target_node_storage'),
	(job->>'change')
)
WHERE state NOT IN ('completed', 'cancelled', 'dead')
			`,
		},
		Down:                 []string{"DROP INDEX replication_queue_target_index"},
		DisableTransactionUp: true,
	}

	allMigrations = append(allMigrations, m)
}
