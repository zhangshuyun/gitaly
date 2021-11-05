package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20210906145021_link_repository_id",
		Up: []string{
			`-- +migrate StatementBegin
DO $BODY$
    DECLARE
        count_val integer DEFAULT 0;
    BEGIN
        LOOP
            WITH updated_rows AS (
                UPDATE storage_repositories
                SET repository_id = sub.repository_id
                FROM (
                    SELECT storage_repositories.virtual_storage, storage_repositories.storage, storage_repositories.relative_path, repositories.repository_id
                    FROM storage_repositories JOIN repositories USING (virtual_storage, relative_path)
                    WHERE storage_repositories.repository_id IS NULL
                    LIMIT 14
                ) AS sub
                    WHERE   storage_repositories.virtual_storage = sub.virtual_storage
                        AND storage_repositories.storage         = sub.storage
                        AND storage_repositories.relative_path   = sub.relative_path
                    RETURNING storage_repositories.repository_id
            )
            SELECT COUNT(*) INTO count_val FROM updated_rows;
            EXIT WHEN count_val = 0;
        END LOOP;
    END
$BODY$
LANGUAGE plpgsql
-- +migrate StatementEnd`,
			`
UPDATE repository_assignments
SET repository_id = repositories.repository_id
FROM repositories
WHERE repository_assignments.virtual_storage = repositories.virtual_storage
AND   repository_assignments.relative_path   = repositories.relative_path
			`,
		},
		Down: []string{},
	}

	allMigrations = append(allMigrations, m)
}
