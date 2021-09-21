package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id:   "20210906130405_add_replica_path",
		Up:   []string{"ALTER TABLE repositories ADD COLUMN replica_path TEXT"},
		Down: []string{"ALTER TABLE repositories DROP COLUMN replica_path"},
	}

	allMigrations = append(allMigrations, m)
}
