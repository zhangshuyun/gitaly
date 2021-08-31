package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20201126165633_repository_assignments_table",
		Up: []string{
			`
			CREATE TABLE repository_assignments (
				virtual_storage TEXT,
				relative_path TEXT,
				storage TEXT,
				PRIMARY KEY (virtual_storage, relative_path, storage),
				FOREIGN KEY (virtual_storage, relative_path) REFERENCES repositories ON DELETE CASCADE ON UPDATE CASCADE
			)`,
		},
		Down: []string{
			"DROP TABLE repository_assignments",
		},
	}

	allMigrations = append(allMigrations, m)
}
