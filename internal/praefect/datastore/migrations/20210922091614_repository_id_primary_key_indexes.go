package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20210922091614_repository_id_primary_key_indexes",
		Up: []string{
			"CREATE UNIQUE INDEX repository_assignments_new_pkey ON repository_assignments (repository_id, storage)",
			"CREATE UNIQUE INDEX storage_repositories_new_pkey ON storage_repositories (repository_id, storage)",
		},
		Down: []string{
			"DROP INDEX repository_assignments_new_pkey",
			"DROP INDEX storage_repositories_new_pkey",
		},
	}

	allMigrations = append(allMigrations, m)
}
