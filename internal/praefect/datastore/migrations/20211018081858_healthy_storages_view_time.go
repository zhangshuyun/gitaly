package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20211018081858_healthy_storages_view_time",
		Up: []string{
			// Re-create view to update usage of the timestamps.
			// We should always rely on the UTC time zone to make timestamps consistent
			// throughout the schema.
			`
CREATE OR REPLACE VIEW healthy_storages AS
	SELECT shard_name AS virtual_storage, node_name AS storage
	FROM node_status AS ns
	WHERE last_seen_active_at >= CLOCK_TIMESTAMP() AT TIME ZONE 'UTC' - INTERVAL '10 SECOND'
	GROUP BY shard_name, node_name
	HAVING COUNT(praefect_name) >= (
		SELECT CEIL(COUNT(DISTINCT praefect_name) / 2.0) AS quorum_count
		FROM node_status
		WHERE shard_name = ns.shard_name
		AND last_contact_attempt_at >= CLOCK_TIMESTAMP() AT TIME ZONE 'UTC' - INTERVAL '60 SECOND'
	)
	ORDER BY shard_name, node_name`,
		},
		Down: []string{
			`
CREATE OR REPLACE VIEW healthy_storages AS
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
	ORDER BY shard_name, node_name`,
		},
	}

	allMigrations = append(allMigrations, m)
}
