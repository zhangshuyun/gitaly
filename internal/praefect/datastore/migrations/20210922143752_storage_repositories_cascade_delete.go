package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20210922143752_storage_repositories_cascade_delete",
		Up: []string{
			"ALTER TABLE storage_repositories DROP CONSTRAINT storage_repositories_repository_id_fkey",
			"ALTER TABLE storage_repositories ADD FOREIGN KEY (repository_id) REFERENCES repositories (repository_id) ON DELETE CASCADE",
		},
		Down: []string{
			"ALTER TABLE storage_repositories DROP CONSTRAINT storage_repositories_repository_id_fkey",
			"ALTER TABLE storage_repositories ADD FOREIGN KEY (repository_id) REFERENCES repositories (repository_id) ON DELETE SET NULL",
		},
	}

	allMigrations = append(allMigrations, m)
}
