package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20200324001604_add_shard_elections_table",
		Up: []string{`CREATE TABLE shard_elections (
			is_primary boolean DEFAULT 'true' NOT NULL,
			shard_name varchar(255) NOT NULL,
			node_name varchar(255) NOT NULL,
			last_seen_active timestamp NOT NULL
		  )`,
			"CREATE UNIQUE INDEX primary_shard_idx ON shard_elections (is_primary, shard_name)",
		},
		Down: []string{"DROP TABLE shard_elections"},
	}

	allMigrations = append(allMigrations, m)
}
