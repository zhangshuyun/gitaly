package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20210923155827_valid_primaries_view_repository_id",
		Up: []string{
			// CREATE OR REPLACE VIEW is not used here as it's not possible to change the columns of a view with it. Dropping and
			// recreating the view allows for that.
			"DROP VIEW valid_primaries",
			`
CREATE VIEW valid_primaries AS
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
		Down: []string{
			"DROP VIEW valid_primaries",
			`
CREATE VIEW valid_primaries AS
	SELECT virtual_storage, relative_path, storage
	FROM (
		SELECT
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
