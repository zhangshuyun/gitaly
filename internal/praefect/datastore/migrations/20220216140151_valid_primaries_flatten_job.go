package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20220216140151_valid_primaries_flatten_job",
		Up: []string{
			`
CREATE OR REPLACE VIEW valid_primaries AS
	SELECT repository_id, virtual_storage, relative_path, storage
	FROM (
		SELECT
			repository_id,
			repositories.virtual_storage,
			repositories.relative_path,
			storage,
			repository_assignments.storage IS NOT NULL
				OR bool_and(repository_assignments.storage IS NULL) OVER (PARTITION BY repository_id) AS eligible
		FROM repositories
		JOIN (SELECT repository_id, storage, generation FROM storage_repositories) AS storage_repositories USING (repository_id, generation)
		JOIN healthy_storages USING (virtual_storage, storage)
		LEFT JOIN repository_assignments USING (repository_id, storage)
		WHERE NOT EXISTS (
			SELECT FROM replication_queue AS queue
			WHERE queue.state NOT IN ('completed', 'dead', 'cancelled')
			AND queue.change = 'delete_replica'
			AND queue.repository_id = storage_repositories.repository_id
			AND queue.target_node_storage = storage_repositories.storage
		)
	) AS candidates
	WHERE eligible
			`,
		},
		Down: []string{
			`
CREATE OR REPLACE VIEW valid_primaries AS
	SELECT repository_id, virtual_storage, relative_path, storage
	FROM (
		SELECT
			repository_id,
			repositories.virtual_storage,
			repositories.relative_path,
			storage,
			repository_assignments.storage IS NOT NULL
				OR bool_and(repository_assignments.storage IS NULL) OVER (PARTITION BY repository_id) AS eligible
		FROM repositories
		JOIN (SELECT repository_id, storage, generation FROM storage_repositories) AS storage_repositories USING (repository_id, generation)
		JOIN healthy_storages USING (virtual_storage, storage)
		LEFT JOIN repository_assignments USING (repository_id, storage)
		WHERE NOT EXISTS (
			SELECT FROM replication_queue AS queue
			WHERE queue.state NOT IN ('completed', 'dead', 'cancelled')
			AND queue.job->>'change' = 'delete_replica'
			AND (queue.job->>'repository_id')::bigint = storage_repositories.repository_id
			AND queue.job->>'target_node_storage' = storage_repositories.storage
		)
	) AS candidates
	WHERE eligible
			`,
		},
	}

	allMigrations = append(allMigrations, m)
}
