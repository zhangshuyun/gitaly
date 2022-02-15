package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20220215160117_replication_queue_cleanup_again",
		Up: []string{
			`DELETE FROM replication_queue WHERE state = ANY (ARRAY['dead'::REPLICATION_JOB_STATE, 'completed'::REPLICATION_JOB_STATE])`,
		},
		Down: []string{
			// there is no way to restore deleted data
		},
	}

	allMigrations = append(allMigrations, m)
}
