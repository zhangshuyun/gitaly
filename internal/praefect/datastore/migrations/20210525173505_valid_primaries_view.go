package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20210525173505_valid_primaries_view",
		Up: []string{
			`
CREATE VIEW repository_generations AS
	SELECT virtual_storage, relative_path, MAX(generation) AS generation
	FROM storage_repositories
	GROUP BY virtual_storage, relative_path
			`,
			`
CREATE VIEW valid_primaries AS
	WITH candidates AS (
		SELECT virtual_storage, relative_path, storage, repository_assignments.storage IS NOT NULL AS assigned
		FROM storage_repositories
		JOIN repository_generations USING (virtual_storage, relative_path, generation)
		JOIN healthy_storages USING (virtual_storage, storage)
		LEFT JOIN repository_assignments USING (virtual_storage, relative_path, storage)
		WHERE NOT EXISTS (
			-- This check exists to prevent us from electing a primary that is pending deletion. The primary
			-- could accept a write and lose it when the deletion is carried out.
			SELECT true
			FROM replication_queue
			WHERE state NOT IN ('completed', 'dead', 'cancelled')
			AND job->>'change' = 'delete_replica'
			AND job->>'virtual_storage' = virtual_storage
			AND job->>'relative_path' = relative_path
			AND job->>'target_node_storage' = storage
		)
	)

	SELECT virtual_storage, relative_path, storage
	FROM candidates
	WHERE assigned OR (
		SELECT NOT EXISTS (
			SELECT FROM candidates AS assigned_candidates
			WHERE assigned
			AND assigned_candidates.virtual_storage = candidates.virtual_storage
			AND assigned_candidates.relative_path   = candidates.relative_path
		)
	)
						`,
		},
		Down: []string{
			"DROP VIEW valid_primaries",
			"DROP VIEW repository_generations",
		},
	}

	allMigrations = append(allMigrations, m)
}
