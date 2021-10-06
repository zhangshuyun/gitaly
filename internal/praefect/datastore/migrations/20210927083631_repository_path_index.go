package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20210927083631_repository_path_index",
		Up: []string{
			`CREATE INDEX CONCURRENTLY repository_replica_path_index
			ON repositories (replica_path, virtual_storage)`,
		},
		DisableTransactionUp: true,
		Down: []string{
			`DROP INDEX repository_replica_path_index`,
		},
	}

	allMigrations = append(allMigrations, m)
}
