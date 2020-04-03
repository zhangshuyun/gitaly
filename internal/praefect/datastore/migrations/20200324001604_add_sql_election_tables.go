package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20200324001604_add_sql_election_tables",
		Up: []string{
			`CREATE TABLE node_status (
				id BIGSERIAL PRIMARY KEY,
				praefect_name VARCHAR(511) NOT NULL,
				shard_name VARCHAR(255) NOT NULL,
				node_name VARCHAR(255) NOT NULL,
				last_contact_attempt_at TIMESTAMP WITH TIME ZONE,
				last_seen_active_at TIMESTAMP WITH TIME ZONE)`,
			"CREATE UNIQUE INDEX shard_node_names_on_node_status_idx ON node_status (praefect_name, shard_name, node_name)",
			"CREATE INDEX shard_name_on_node_status_idx ON node_status (shard_name, node_name)",

			`CREATE TABLE shard_primaries (
				id BIGSERIAL PRIMARY KEY,
				shard_name VARCHAR(255) NOT NULL,
				node_name VARCHAR(255) NOT NULL,
				elected_by_praefect VARCHAR(255) NOT NULL,
				elected_at TIMESTAMP WITH TIME ZONE NOT NULL)`,
			"CREATE UNIQUE INDEX shard_name_on_shard_primaries_idx ON shard_primaries (shard_name)",
		},
		Down: []string{
			"DROP TABLE shard_primaries",
			"DROP TABLE node_status",
		},
	}

	allMigrations = append(allMigrations, m)
}
