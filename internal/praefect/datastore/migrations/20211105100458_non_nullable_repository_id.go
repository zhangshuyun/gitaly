package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20211105100458_non_nullable_repository_id",
		Up: []string{
			// Disable the trigger to avoid notifications being sent for the delete below.
			// No cache invalidation is necessary as the repository being deleted would have invalidated
			// the cache and it wouldn't have been filled afterwards as the repository doesn't exist.
			"ALTER TABLE storage_repositories DISABLE TRIGGER notify_on_delete",
			// Remove orphaned replica records. 20210922143752_storage_repositories_cascade_delete introduced also
			// in 14.5 changed the repository_id foreign key to cascade deletes from the `repositories` table.
			// With the migration applied, the cascade in the schema guarantees that even older version of Praefect
			// do not leave orphaned records around anymore when deleting repositories. This ensures concurrent deletions
			// performed by older Praefects won't leave any orphaned records which would prevent the NOT NULL constraint
			// from being set. This allows us to already clean up the old state in the same release the behavior change
			// was introduced.
			"DELETE FROM storage_repositories WHERE repository_id IS NULL",
			// Enable the triggers again after the deletion is done.
			"ALTER TABLE storage_repositories ENABLE TRIGGER notify_on_delete",
			// With all the orphaned records gone, make the schema stricter and make the repository id
			// non nullable.
			"ALTER TABLE storage_repositories ALTER COLUMN repository_id SET NOT NULL",
			"ALTER TABLE repository_assignments ALTER COLUMN repository_id SET NOT NULL",
			// Drop the non validated constraint now that the column is set to NOT NULL.
			"ALTER TABLE storage_repositories DROP CONSTRAINT repository_id_not_null",
			"ALTER TABLE repository_assignments DROP CONSTRAINT repository_id_not_null",
		},
		Down: []string{
			"ALTER TABLE repository_assignments ADD CONSTRAINT repository_id_not_null CHECK (repository_id IS NOT NULL) NOT VALID",
			"ALTER TABLE storage_repositories ADD CONSTRAINT repository_id_not_null CHECK (repository_id IS NOT NULL) NOT VALID",
			"ALTER TABLE storage_repositories ALTER COLUMN repository_id DROP NOT NULL",
			"ALTER TABLE repository_assignments ALTER COLUMN repository_id DROP NOT NULL",
		},
	}

	allMigrations = append(allMigrations, m)
}
