package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id:                   "20220216131417_indexes_on_replication_queue",
		DisableTransactionUp: true,
		Up: []string{
			`DROP INDEX replication_queue_target_index`,
			`CREATE INDEX CONCURRENTLY replication_queue_target_index
			ON replication_queue (virtual_storage, relative_path, target_node_storage, change)
			WHERE state NOT IN ('completed', 'cancelled', 'dead')`,

			`DROP INDEX delete_replica_unique_index`,
			`CREATE UNIQUE INDEX CONCURRENTLY delete_replica_unique_index
			ON replication_queue (virtual_storage, relative_path)
			WHERE state NOT IN ('completed', 'cancelled', 'dead')
			AND change = 'delete_replica'`,

			`DROP INDEX IF EXISTS virtual_target_on_replication_queue_idx`,
			`CREATE INDEX CONCURRENTLY virtual_target_on_replication_queue_idx
				ON replication_queue USING BTREE (virtual_storage, target_node_storage)`,
		},
		DisableTransactionDown: true,
		Down: []string{
			`DROP INDEX replication_queue_target_index`,
			`CREATE INDEX CONCURRENTLY replication_queue_target_index
			ON replication_queue (
				(job->>'virtual_storage'),
				(job->>'relative_path'),
				(job->>'target_node_storage'),
				(job->>'change')
			)
			WHERE state NOT IN ('completed', 'cancelled', 'dead')`,

			`DROP INDEX delete_replica_unique_index`,
			`CREATE UNIQUE INDEX CONCURRENTLY delete_replica_unique_index
			ON replication_queue (
				(job->>'virtual_storage'),
				(job->>'relative_path')
			)
			WHERE state NOT IN ('completed', 'cancelled', 'dead')
			AND job->>'change' = 'delete_replica'`,

			`DROP INDEX IF EXISTS virtual_target_on_replication_queue_idx`,
			`CREATE INDEX CONCURRENTLY virtual_target_on_replication_queue_idx
				ON replication_queue USING BTREE ((job->>'virtual_storage'), (job->>'target_node_storage'))`,
		},
	}

	allMigrations = append(allMigrations, m)
}
