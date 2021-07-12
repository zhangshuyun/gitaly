package testhelper

import (
	"context"
	"testing"

	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
)

// SetHealthyNodes sets the healthy nodes in the database as determined by the passed in map. The healthyNodes map is keyed by
// praefect name -> virtual storage -> storage. On each run, it clears all previous health checks from the table, so the
// passed in nodes are the only ones considered healthy after the function. As the healthy nodes are determined by the time of
// the last successful health check, this should be run in the same transastion as the tested query to prevent flakiness.
//
//nolint:golint
func SetHealthyNodes(t testing.TB, ctx context.Context, db glsql.Querier, healthyNodes map[string]map[string][]string) {
	t.Helper()

	var praefects, virtualStorages, storages []string
	for praefect, virtualStors := range healthyNodes {
		for virtualStorage, stors := range virtualStors {
			for _, storage := range stors {
				praefects = append(praefects, praefect)
				virtualStorages = append(virtualStorages, virtualStorage)
				storages = append(storages, storage)
			}
		}
	}

	_, err := db.ExecContext(ctx, `
WITH clear_previous_checks AS ( DELETE FROM node_status )

INSERT INTO node_status (praefect_name, shard_name, node_name, last_contact_attempt_at, last_seen_active_at)
SELECT
	unnest($1::text[]) AS praefect_name,
	unnest($2::text[]) AS shard_name,
	unnest($3::text[]) AS node_name,
	NOW() AS last_contact_attempt_at,
	NOW() AS last_seen_active_at
ON CONFLICT (praefect_name, shard_name, node_name) DO UPDATE SET
	last_contact_attempt_at = NOW(),
	last_seen_active_at = NOW()
		`,
		pq.StringArray(praefects),
		pq.StringArray(virtualStorages),
		pq.StringArray(storages),
	)
	require.NoError(t, err)
}
