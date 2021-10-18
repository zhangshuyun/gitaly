package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20211018142158_valid_primaries_view_join_repo_id",
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
			SELECT FROM replication_queue
			WHERE state NOT IN ('completed', 'dead', 'cancelled')
			AND job->>'change' = 'delete_replica'
			AND (job->>'repository_id')::bigint = repository_id
			AND job->>'target_node_storage' = storage
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
			storage_repositories.repository_id,
			virtual_storage,
			relative_path,
			storage,
			repository_assignments.storage IS NOT NULL
				OR bool_and(repository_assignments.storage IS NULL) OVER (PARTITION BY virtual_storage, relative_path) AS eligible
		FROM storage_repositories
		JOIN repository_generations USING (virtual_storage, relative_path, generation)
		JOIN healthy_storages USING (virtual_storage, storage)
		LEFT JOIN repository_assignments USING (virtual_storage, relative_path, storage)
		WHERE NOT EXISTS (
			SELECT FROM replication_queue
			WHERE state NOT IN ('completed', 'dead', 'cancelled')
			AND job->>'change' = 'delete_replica'
			AND job->>'virtual_storage' = virtual_storage
			AND job->>'relative_path' = relative_path
			AND job->>'target_node_storage' = storage
		)
	) AS candidates
	WHERE eligible
			`,
		},
	}

	allMigrations = append(allMigrations, m)
}
