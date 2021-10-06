package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id:   "20210921131816_backfill_replica_path",
		Up:   []string{"UPDATE repositories SET replica_path = relative_path"},
		Down: []string{},
	}

	allMigrations = append(allMigrations, m)
}
