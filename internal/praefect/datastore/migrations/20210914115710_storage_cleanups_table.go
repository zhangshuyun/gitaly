package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20210914115710_storage_cleanups_table",
		Up: []string{
			`CREATE TABLE storage_cleanups (
				virtual_storage TEXT NOT NULL,
    				storage TEXT NOT NULL,
				last_run TIMESTAMP WITHOUT TIME ZONE,
				triggered_at TIMESTAMP WITHOUT TIME ZONE,
				PRIMARY KEY (virtual_storage, storage)
			)`,
		},
		Down: []string{
			`DROP TABLE storage_cleanups`,
		},
	}

	allMigrations = append(allMigrations, m)
}
