// +build postgres

package reconciler

import (
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func BenchmarkReconcile(b *testing.B) {
	for _, numberOfRepositories := range []int{1000, 10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("%d", numberOfRepositories), func(b *testing.B) {
			b.Run("best case", func(b *testing.B) {
				benchmarkReconcile(b, numberOfRepositories, false)
			})

			b.Run("worst case", func(b *testing.B) {
				benchmarkReconcile(b, numberOfRepositories, true)
			})
		})
	}
}

func benchmarkReconcile(b *testing.B, numRepositories int, worstCase bool) {
	b.StopTimer()

	ctx, cancel := testhelper.Context()
	defer cancel()

	db := getDB(b)

	behind := 0
	if worstCase {
		// 2 out of 3 storages will be outdated and in need of replication
		behind = 1
	}

	_, err := db.ExecContext(ctx, `
WITH repositories AS (
	INSERT INTO repositories
	SELECT 'virtual-storage-1', 'repository-'|| SERIES.INDEX, 5
	FROM GENERATE_SERIES(1, $1) SERIES(INDEX)
	RETURNING virtual_storage, relative_path, generation
)

INSERT INTO storage_repositories
SELECT
	virtual_storage,
	relative_path,
	storage,
	CASE WHEN storage = 'gitaly-1' THEN generation ELSE generation - $2 END AS generation
FROM repositories
CROSS JOIN (SELECT unnest('{gitaly-1, gitaly-2, gitaly-3}'::text[]) AS storage) AS storages
`, numRepositories, behind)
	require.NoError(b, err)

	storages := map[string][]string{"virtual-storage-1": {"gitaly-1", "gitaly-2", "gitaly-3"}}
	for n := 0; n < b.N; n++ {
		db.Truncate(b, "replication_queue", "replication_queue_lock", "replication_queue_job_lock")
		r := NewReconciler(
			testhelper.DiscardTestLogger(b),
			db,
			praefect.StaticHealthChecker(storages),
			storages,
			prometheus.DefBuckets,
		)

		b.StartTimer()
		err = r.reconcile(ctx)
		b.StopTimer()

		require.NoError(b, err)
	}
}
