package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20210223130233_delete_replica_unique_index",
		Up: []string{
			`
CREATE UNIQUE INDEX CONCURRENTLY delete_replica_unique_index
ON replication_queue (
	(job->>'virtual_storage'),
	(job->>'relative_path')
)
WHERE state NOT IN ('completed', 'cancelled', 'dead')
AND job->>'change' = 'delete_replica'
			`,
		},
		Down:                 []string{"DROP INDEX delete_replica_unique_index"},
		DisableTransactionUp: true,
	}

	allMigrations = append(allMigrations, m)
}
