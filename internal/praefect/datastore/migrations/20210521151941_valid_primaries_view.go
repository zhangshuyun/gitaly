package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20210521151941_valid_primaries_view",
		Up: []string{
			`
CREATE VIEW replicas AS
	SELECT * FROM storage_repositories
`,

			`
CREATE VIEW healthy_storages AS
	SELECT shard_name AS virtual_storage, node_name AS storage
	FROM node_status AS ns
	WHERE last_seen_active_at >= NOW() - INTERVAL '10 SECOND'
	GROUP BY shard_name, node_name
	HAVING COUNT(praefect_name) >= (
		SELECT CEIL(COUNT(DISTINCT praefect_name) / 2.0) AS quorum_count
		FROM node_status
		WHERE shard_name = ns.shard_name
		AND last_contact_attempt_at >= NOW() - INTERVAL '60 SECOND'
	)
	ORDER BY shard_name, node_name
`,

			`
CREATE VIEW replicas_pending_deletion AS
	SELECT
		job->>'virtual_storage' AS virtual_storage,
		job->>'relative_path' AS relative_path,
		job->>'target_node_storage' AS storage
	FROM replication_queue
	WHERE state NOT IN ('completed', 'dead', 'cancelled')
	AND job->>'change' in ('delete_replica', 'delete')
`,

			`
CREATE VIEW valid_primaries AS
	SELECT virtual_storage, relative_path, storage
	FROM repositories
	JOIN healthy_storages USING (virtual_storage)
	JOIN replicas USING (virtual_storage, relative_path, storage)
	WHERE (
		-- If assignments exist for the repository, only the assigned storages elected as primary.
		-- If no assignments exist, any healthy node can be elected as the primary
		SELECT COUNT(*) = 0 OR COUNT(*) FILTER (WHERE storage = healthy_storages.storage) = 1
		FROM repository_assignments
		WHERE repository_assignments.virtual_storage = repositories.virtual_storage
		AND repository_assignments.relative_path = repositories.relative_path
	) AND NOT EXISTS (
		SELECT FROM replicas_pending_deletion
		WHERE virtual_storage = repositories.virtual_storage
		AND relative_path = repositories.relative_path
		AND storage = healthy_storages.storage
	)
`,

			`
CREATE VIEW repository_generations AS
	SELECT virtual_storage, relative_path, MAX(generation) AS generation
	FROM replicas
	GROUP BY virtual_storage, relative_path
`,
			`
CREATE VIEW in_sync_replicas AS
	SELECT virtual_storage, relative_path, storage
	FROM (SELECT virtual_storage, relative_path FROM repositories) AS repositories
	JOIN replicas USING (virtual_storage, relative_path)
	JOIN repository_generations USING (virtual_storage, relative_path, generation)
`,

			`
CREATE VIEW outdated_replicas AS
	SELECT virtual_storage, relative_path, storage
	FROM replicas
	WHERE NOT EXISTS (
		SELECT FROM in_sync_replicas
		WHERE virtual_storage = replicas.virtual_storage
		AND relative_path = replicas.relative_path
		AND storage = replicas.storage
	)
`,
		},
		Down: []string{"DROP VIEW valid_primaries"},
	}

	allMigrations = append(allMigrations, m)
}
