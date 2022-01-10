package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20210906145021_link_repository_id",
		Up: []string{
			"ALTER TABLE storage_repositories DISABLE TRIGGER notify_on_update",
			`
UPDATE storage_repositories
SET repository_id = repositories.repository_id
FROM repositories
WHERE storage_repositories.virtual_storage = repositories.virtual_storage
AND   storage_repositories.relative_path   = repositories.relative_path
            `,
			`
UPDATE repository_assignments
SET repository_id = repositories.repository_id
FROM repositories
WHERE repository_assignments.virtual_storage = repositories.virtual_storage
AND   repository_assignments.relative_path   = repositories.relative_path
            `,
			"ALTER TABLE storage_repositories ENABLE TRIGGER notify_on_update",
		},
		Down: []string{},
	}

	allMigrations = append(allMigrations, m)
}
