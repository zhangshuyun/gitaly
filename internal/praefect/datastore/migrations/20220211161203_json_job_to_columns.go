package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20220211161203_json_job_to_columns",
		Up: []string{
			// 1. The replication_queue was extended with set of the new columns to cover
			// job JSON struct. This query update all existing rows to fulfil newly
			// added columns with data from the job column.
			`UPDATE replication_queue SET
				change = (queue.job->>'change')::REPLICATION_JOB_TYPE,
				repository_id = COALESCE(
					-- For the old jobs the repository_id may be 'null' as we didn't have migration that
					-- populates those with the value from the repositories table.
					-- The repository_id also may be a 0 in case the job was created before repositories table was fulfilled
					-- and event was created by reconciler.
					CASE WHEN (queue.job->>'repository_id')::BIGINT = 0 THEN NULL ELSE (queue.job->>'repository_id')::BIGINT END,
					(SELECT repositories.repository_id FROM repositories WHERE repositories.virtual_storage = queue.job->>'virtual_storage' AND repositories.relative_path = queue.job->>'relative_path'),
					-- If repository_id doesn't exist we still need to fill this column otherwise it may fail on scan into struct.
					0),
				replica_path = COALESCE(queue.job->>'replica_path', ''),
				relative_path = queue.job->>'relative_path',
				target_node_storage = queue.job->>'target_node_storage',
				source_node_storage = COALESCE(queue.job->>'source_node_storage', ''),
				virtual_storage = queue.job->>'virtual_storage',
				params = (queue.job->>'params')::JSONB
			FROM replication_queue AS queue
			LEFT JOIN repositories ON
				queue.job->>'virtual_storage' = repositories.virtual_storage AND
				queue.job->>'relative_path' = repositories.relative_path
			WHERE replication_queue.id = queue.id`,

			// 2. Drop existing foreign key as it does nothing on removal of the referencable row.
			`ALTER TABLE replication_queue_job_lock
				DROP CONSTRAINT replication_queue_job_lock_job_id_fkey`,

			// 3. And re-create it with the cascade deletion.
			`ALTER TABLE replication_queue_job_lock
				ADD CONSTRAINT replication_queue_job_lock_job_id_fkey
					FOREIGN KEY (job_id)
					REFERENCES replication_queue(id)
					ON DELETE CASCADE`,

			// 4. Drop existing foreign key as it does nothing on removal of the referencable row.
			`ALTER TABLE replication_queue_job_lock DROP CONSTRAINT replication_queue_job_lock_lock_id_fkey`,

			// 5. And re-create it with the cascade deletion.
			`ALTER TABLE replication_queue_job_lock
				ADD CONSTRAINT replication_queue_job_lock_lock_id_fkey
					FOREIGN KEY (lock_id)
					REFERENCES replication_queue_lock(id)
					ON DELETE CASCADE`,

			// 6. The replication events without repository_id are too old and should be
			// removed for the foreign key constraint to be created on the repositories table.
			`DELETE FROM replication_queue
			WHERE repository_id = 0`,

			// 7. In case we removed some in_progress replication events we need to remove
			// corresponding rows from replication_queue_lock because repository doesn't
			// exist anymore and we don't need a lock row for it.
			`DELETE FROM replication_queue_lock
			WHERE acquired
				AND NOT EXISTS(SELECT FROM replication_queue_job_lock WHERE lock_id = id)`,

			// 8. Once the repository is removed (row deleted from repositories table) it
			// can't be used anymore. And there is no reason to process any remaining
			// replication events. If gitaly node was out of service and repository
			// deletion was scheduled as replication job this job will be removed as well
			// but https://gitlab.com/gitlab-org/gitaly/-/issues/3719 should deal with it.
			// And to automatically cleanup remaining replication events we create a foreign
			// key with cascade removal.
			`ALTER TABLE replication_queue
				ADD CONSTRAINT replication_queue_repository_id_fkey
					FOREIGN KEY (repository_id)
					REFERENCES repositories(repository_id)
					ON DELETE CASCADE`,

			// 9. If repository is removed there is nothing that cleans up replication_queue_lock
			// table.The table is used to sync run of the replication jobs and rows in it
			// created before rows in the replication_queue table that is why we can't use
			// foreign key to remove orphaned lock rows. We rely on the trigger that removes
			// rows from replication_queue_lock once the record is removed from the repositories table.
			`-- +migrate StatementBegin
			CREATE FUNCTION remove_queue_lock_on_repository_removal() RETURNS TRIGGER AS $$
			BEGIN
				DELETE FROM replication_queue_lock
				WHERE id LIKE (OLD.virtual_storage || '|%|' || OLD.relative_path);
				RETURN NULL;
		    	END;
			$$ LANGUAGE plpgsql;`,

			// 10. Activates a trigger to remove rows from the replication_queue_lock once the row
			// is deleted from the repositories table .
			`CREATE TRIGGER remove_queue_lock_on_repository_removal AFTER DELETE ON repositories
    				FOR EACH ROW EXECUTE PROCEDURE remove_queue_lock_on_repository_removal()`,
		},
		Down: []string{
			// 10.
			`DROP TRIGGER remove_queue_lock_on_repository_removal ON repositories`,

			// 9.
			`DROP FUNCTION remove_queue_lock_on_repository_removal`,

			// 8.
			`ALTER TABLE replication_queue
				DROP CONSTRAINT replication_queue_repository_id_fkey`,

			// 7. We can't restore deleted rows, nothing to do here.

			// 6. We can't restore deleted rows, nothing to do here.

			// 5.
			`ALTER TABLE replication_queue_job_lock DROP CONSTRAINT replication_queue_job_lock_lock_id_fkey`,

			// 4. Re-create foreign key with the default options.
			`ALTER TABLE replication_queue_job_lock
				ADD CONSTRAINT replication_queue_job_lock_lock_id_fkey
					FOREIGN KEY (lock_id)
					REFERENCES replication_queue_lock(id)`,

			// 3.
			`ALTER TABLE replication_queue_job_lock
				DROP CONSTRAINT replication_queue_job_lock_job_id_fkey`,

			// 2.
			`ALTER TABLE replication_queue_job_lock
				ADD CONSTRAINT replication_queue_job_lock_job_id_fkey
					FOREIGN KEY (job_id)
					REFERENCES replication_queue(id)`,

			// 1. We don't know what is the set of rows we updated that is why we can't reset
			// them back, so nothing to do here.
		},
	}

	allMigrations = append(allMigrations, m)
}
