package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20210727085659_repository_ids",
		Up: []string{
			"CREATE UNIQUE INDEX repository_lookup_index ON repositories (virtual_storage, relative_path)",
			"ALTER TABLE repository_assignments DROP CONSTRAINT IF EXISTS repository_assignments_virtual_storage_relative_path_fkey",
			"ALTER TABLE repository_assignments DROP CONSTRAINT IF EXISTS repository_assignments_virtual_storage_fkey",
			"ALTER TABLE repositories DROP CONSTRAINT repositories_pkey",
			"ALTER TABLE repository_assignments ADD CONSTRAINT repository_assignments_virtual_storage_relative_path_fkey FOREIGN KEY (virtual_storage, relative_path) REFERENCES repositories (virtual_storage, relative_path) ON UPDATE CASCADE ON DELETE CASCADE",
			"ALTER TABLE repositories ADD COLUMN repository_id BIGSERIAL PRIMARY KEY",
			"ALTER TABLE repository_assignments ADD COLUMN repository_id BIGINT REFERENCES repositories (repository_id) ON DELETE CASCADE",
			"ALTER TABLE storage_repositories ADD COLUMN repository_id BIGINT REFERENCES repositories (repository_id) ON DELETE SET NULL",
		},
		Down: []string{
			"ALTER TABLE storage_repositories DROP COLUMN repository_id",
			"ALTER TABLE repository_assignments DROP COLUMN repository_id",
			"ALTER TABLE repositories DROP COLUMN repository_id",
			"CREATE UNIQUE INDEX repositories_pkey ON repositories (virtual_storage, relative_path)",
			"ALTER TABLE repositories ADD PRIMARY KEY USING INDEX repositories_pkey",
			"ALTER TABLE repository_assignments DROP CONSTRAINT repository_assignments_virtual_storage_relative_path_fkey",
			"DROP INDEX repository_lookup_index",
			"ALTER TABLE repository_assignments ADD FOREIGN KEY (virtual_storage, relative_path) REFERENCES repositories (virtual_storage, relative_path) ON UPDATE CASCADE ON DELETE CASCADE",
		},
	}

	allMigrations = append(allMigrations, m)
}
