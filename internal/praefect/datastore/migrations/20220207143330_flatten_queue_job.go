package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20220207143330_flatten_queue_job",
		Up: []string{
			`CREATE TYPE REPLICATION_JOB_TYPE AS ENUM('update','create','delete','delete_replica','rename','gc','repack_full','repack_incremental','cleanup','pack_refs','write_commit_graph','midx_repack','optimize_repository', 'prune_unreachable_objects')`,

			// The repository_id, replica_path and source_node_storage columns could have
			// NULL values for an existing events, but because a non-pointer receivers
			// are used to scan them in go code we should prevent situation when NULL is
			// returned from the database as scan will fail in that case.
			`ALTER TABLE replication_queue
				ADD COLUMN change REPLICATION_JOB_TYPE,
				ADD COLUMN repository_id BIGINT NOT NULL DEFAULT 0,
				ADD COLUMN replica_path TEXT  NOT NULL DEFAULT '',
				ADD COLUMN relative_path TEXT,
				ADD COLUMN target_node_storage TEXT,
				ADD COLUMN source_node_storage TEXT NOT NULL DEFAULT '',
				ADD COLUMN virtual_storage TEXT,
				ADD COLUMN params JSONB`,

			`-- +migrate StatementBegin
			CREATE OR REPLACE FUNCTION replication_queue_flatten_job() RETURNS TRIGGER AS $$
			BEGIN
				NEW.change := (NEW.job->>'change')::REPLICATION_JOB_TYPE;
				NEW.repository_id := COALESCE(
					-- For the old jobs the repository_id field may be 'null' or not set at all.
					-- repository_id field could have 0 if event was created by reconciler before
					-- repositories table was populated with valid repository_id.
					CASE WHEN (NEW.job->>'repository_id')::BIGINT = 0 THEN NULL ELSE (NEW.job->>'repository_id')::BIGINT END,
					(SELECT repositories.repository_id FROM repositories WHERE repositories.virtual_storage = NEW.job->>'virtual_storage' AND repositories.relative_path = NEW.job->>'relative_path'),
					0
				);
				-- The reconciler doesn't populate replica_path field that is why we need make sure
				-- we have at least an empty value for the column, not to break scan operations.
				NEW.replica_path := COALESCE(NEW.job->>'replica_path', '');
				NEW.relative_path := NEW.job->>'relative_path';
				NEW.target_node_storage := NEW.job->>'target_node_storage';
				NEW.source_node_storage := COALESCE(NEW.job->>'source_node_storage', '');
				NEW.virtual_storage := NEW.job->>'virtual_storage';
				NEW.params := (NEW.job->>'params')::JSONB;
				RETURN NEW;
			END;
			$$ LANGUAGE plpgsql;
			-- +migrate StatementEnd`,

			`CREATE TRIGGER replication_queue_flatten_job BEFORE INSERT ON replication_queue
				FOR EACH ROW EXECUTE PROCEDURE replication_queue_flatten_job()`,
		},
		Down: []string{
			`DROP TRIGGER replication_queue_flatten_job ON replication_queue`,

			`DROP FUNCTION replication_queue_flatten_job`,

			`ALTER TABLE replication_queue
				DROP COLUMN change,
				DROP COLUMN repository_id,
				DROP COLUMN replica_path,
				DROP COLUMN relative_path,
				DROP COLUMN target_node_storage,
				DROP COLUMN source_node_storage,
				DROP COLUMN virtual_storage,
				DROP COLUMN params`,

			`DROP TYPE REPLICATION_JOB_TYPE`,
		},
	}

	allMigrations = append(allMigrations, m)
}
