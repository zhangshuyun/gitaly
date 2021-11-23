package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20211105100456_check_non_null_repository_id",
		Up: []string{
			// Set a non-validated NOT NULL constraint to ensure no new entries are inserted during the delete statement in the
			// next migration that sets the columns as NOT NULL.
			"ALTER TABLE storage_repositories ADD CONSTRAINT repository_id_not_null CHECK (repository_id IS NOT NULL) NOT VALID",
			"ALTER TABLE repository_assignments ADD CONSTRAINT repository_id_not_null CHECK (repository_id IS NOT NULL) NOT VALID",
		},
		Down: []string{
			"ALTER TABLE storage_repositories DROP CONSTRAINT repository_id_not_null",
			"ALTER TABLE repository_assignments DROP CONSTRAINT repository_id_not_null",
		},
	}

	allMigrations = append(allMigrations, m)
}
