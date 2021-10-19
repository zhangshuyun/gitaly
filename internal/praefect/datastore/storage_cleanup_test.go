package datastore

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestStorageCleanup_Populate(t *testing.T) {
	t.Parallel()
	ctx, cancel := testhelper.Context()
	defer cancel()
	db := glsql.NewDB(t)
	storageCleanup := NewStorageCleanup(db.DB)

	require.NoError(t, storageCleanup.Populate(ctx, "praefect", "gitaly-1"))
	actual := getAllStoragesCleanup(t, ctx, db)
	single := []storageCleanupRow{{ClusterPath: ClusterPath{VirtualStorage: "praefect", Storage: "gitaly-1"}}}
	require.Equal(t, single, actual)

	err := storageCleanup.Populate(ctx, "praefect", "gitaly-1")
	require.NoError(t, err, "population of the same data should not generate an error")
	actual = getAllStoragesCleanup(t, ctx, db)
	require.Equal(t, single, actual, "same data should not create additional rows or change existing")

	require.NoError(t, storageCleanup.Populate(ctx, "default", "gitaly-2"))
	multiple := append(single, storageCleanupRow{ClusterPath: ClusterPath{VirtualStorage: "default", Storage: "gitaly-2"}})
	actual = getAllStoragesCleanup(t, ctx, db)
	require.ElementsMatch(t, multiple, actual, "new data should create additional row")
}

func TestStorageCleanup_AcquireNextStorage(t *testing.T) {
	t.Parallel()
	ctx, cancel := testhelper.Context()
	defer cancel()
	db := glsql.NewDB(t)

	t.Run("ok", func(t *testing.T) {
		tx := db.Begin(t)
		defer tx.Rollback(t)
		storageCleanup := NewStorageCleanup(tx)

		require.NoError(t, storageCleanup.Populate(ctx, "vs", "g1"))

		clusterPath, release, err := storageCleanup.AcquireNextStorage(ctx, 0, time.Second)
		require.NoError(t, err)
		require.NoError(t, release())
		require.Equal(t, &ClusterPath{VirtualStorage: "vs", Storage: "g1"}, clusterPath)
	})

	t.Run("last_run condition", func(t *testing.T) {
		tx := db.Begin(t)
		defer tx.Rollback(t)
		storageCleanup := NewStorageCleanup(tx)

		require.NoError(t, storageCleanup.Populate(ctx, "vs", "g1"))
		// Acquire it to initialize last_run column.
		_, release, err := storageCleanup.AcquireNextStorage(ctx, 0, time.Second)
		require.NoError(t, err)
		require.NoError(t, release())

		clusterPath, release, err := storageCleanup.AcquireNextStorage(ctx, time.Hour, time.Second)
		require.NoError(t, err)
		require.NoError(t, release())
		require.Nil(t, clusterPath, "no result expected as there can't be such entries")
	})

	t.Run("sorting based on storage name as no executions done yet", func(t *testing.T) {
		tx := db.Begin(t)
		defer tx.Rollback(t)
		storageCleanup := NewStorageCleanup(tx)

		require.NoError(t, storageCleanup.Populate(ctx, "vs", "g1"))
		require.NoError(t, storageCleanup.Populate(ctx, "vs", "g2"))
		require.NoError(t, storageCleanup.Populate(ctx, "vs", "g3"))

		clusterPath, release, err := storageCleanup.AcquireNextStorage(ctx, 0, time.Second)
		require.NoError(t, err)
		require.NoError(t, release())
		require.Equal(t, &ClusterPath{VirtualStorage: "vs", Storage: "g1"}, clusterPath)
	})

	t.Run("sorting based on storage name and last_run", func(t *testing.T) {
		tx := db.Begin(t)
		defer tx.Rollback(t)
		storageCleanup := NewStorageCleanup(tx)

		require.NoError(t, storageCleanup.Populate(ctx, "vs", "g1"))
		_, release, err := storageCleanup.AcquireNextStorage(ctx, 0, time.Second)
		require.NoError(t, err)
		require.NoError(t, release())
		require.NoError(t, storageCleanup.Populate(ctx, "vs", "g2"))

		clusterPath, release, err := storageCleanup.AcquireNextStorage(ctx, 0, time.Second)
		require.NoError(t, err)
		require.NoError(t, release())
		require.Equal(t, &ClusterPath{VirtualStorage: "vs", Storage: "g2"}, clusterPath)
	})

	t.Run("sorting based on last_run", func(t *testing.T) {
		tx := db.Begin(t)
		defer tx.Rollback(t)
		storageCleanup := NewStorageCleanup(tx)

		require.NoError(t, storageCleanup.Populate(ctx, "vs", "g1"))
		require.NoError(t, storageCleanup.Populate(ctx, "vs", "g2"))
		clusterPath, release, err := storageCleanup.AcquireNextStorage(ctx, 0, time.Second)
		require.NoError(t, err)
		require.NoError(t, release())
		require.Equal(t, &ClusterPath{VirtualStorage: "vs", Storage: "g1"}, clusterPath)
		clusterPath, release, err = storageCleanup.AcquireNextStorage(ctx, 0, time.Second)
		require.NoError(t, err)
		require.NoError(t, release())
		require.Equal(t, &ClusterPath{VirtualStorage: "vs", Storage: "g2"}, clusterPath)

		clusterPath, release, err = storageCleanup.AcquireNextStorage(ctx, 0, time.Second)
		require.NoError(t, err)
		require.NoError(t, release())
		require.Equal(t, &ClusterPath{VirtualStorage: "vs", Storage: "g1"}, clusterPath)
	})

	t.Run("already acquired won't be acquired until released", func(t *testing.T) {
		tx := db.Begin(t)
		defer tx.Rollback(t)
		storageCleanup := NewStorageCleanup(tx)

		require.NoError(t, storageCleanup.Populate(ctx, "vs", "g1"))
		_, release1, err := storageCleanup.AcquireNextStorage(ctx, 0, time.Second)
		require.NoError(t, err)

		clusterPath, release2, err := storageCleanup.AcquireNextStorage(ctx, 0, time.Second)
		require.NoError(t, err)
		require.Nil(t, clusterPath, clusterPath)
		require.NoError(t, release1())
		require.NoError(t, release2())

		clusterPath, release3, err := storageCleanup.AcquireNextStorage(ctx, 0, time.Second)
		require.NoError(t, err)
		require.NotNil(t, clusterPath)
		require.NoError(t, release3())
	})

	t.Run("already acquired won't be acquired until released", func(t *testing.T) {
		tx := db.Begin(t)
		defer tx.Rollback(t)
		storageCleanup := NewStorageCleanup(tx)

		require.NoError(t, storageCleanup.Populate(ctx, "vs", "g1"))
		_, release1, err := storageCleanup.AcquireNextStorage(ctx, 0, time.Second)
		require.NoError(t, err)

		clusterPath, release2, err := storageCleanup.AcquireNextStorage(ctx, 0, time.Second)
		require.NoError(t, err)
		require.Nil(t, clusterPath, clusterPath)
		require.NoError(t, release1())
		require.NoError(t, release2())

		clusterPath, release3, err := storageCleanup.AcquireNextStorage(ctx, 0, time.Second)
		require.NoError(t, err)
		require.NotNil(t, clusterPath)
		require.NoError(t, release3())
	})

	t.Run("acquired for long time triggers update loop", func(t *testing.T) {
		tx := db.Begin(t)
		defer tx.Rollback(t)
		storageCleanup := NewStorageCleanup(tx)

		require.NoError(t, storageCleanup.Populate(ctx, "vs", "g1"))
		start := time.Now().UTC()
		_, release, err := storageCleanup.AcquireNextStorage(ctx, 0, 200*time.Millisecond)
		require.NoError(t, err)

		// Make sure the triggered_at column has a non NULL value after the record is acquired.
		check1 := getAllStoragesCleanup(t, ctx, tx)
		require.Len(t, check1, 1)
		require.True(t, check1[0].TriggeredAt.Valid)
		require.Truef(t, check1[0].TriggeredAt.Time.After(start), "%s is not after %s", check1[0].TriggeredAt, start)

		// Check the goroutine running in the background updates triggered_at column periodically.
		time.Sleep(time.Second)

		check2 := getAllStoragesCleanup(t, ctx, tx)
		require.Len(t, check2, 1)
		require.True(t, check2[0].TriggeredAt.Valid)
		require.Truef(t, check2[0].TriggeredAt.Time.After(check1[0].TriggeredAt.Time), "%s is not after %s", check2[0].TriggeredAt, check1[0].TriggeredAt)

		require.NoError(t, release())

		// Make sure the triggered_at column has a NULL value after the record is released.
		check3 := getAllStoragesCleanup(t, ctx, tx)
		require.Len(t, check3, 1)
		require.False(t, check3[0].TriggeredAt.Valid)
	})
}

func TestStorageCleanup_Exists(t *testing.T) {
	t.Parallel()
	ctx, cancel := testhelper.Context()
	defer cancel()

	db := glsql.NewDB(t)

	repoStore := NewPostgresRepositoryStore(db.DB, nil)
	const (
		virtualStorage = "vs"
		relativePath1  = "p/1"
		relativePath2  = "p/2"
		storage1       = "g1"
		storage2       = "g2"
		storage3       = "g3"
	)
	id1, err := repoStore.ReserveRepositoryID(ctx, virtualStorage, relativePath1)
	require.NoError(t, err)
	id2, err := repoStore.ReserveRepositoryID(ctx, virtualStorage, relativePath2)
	require.NoError(t, err)

	require.NoError(t, repoStore.CreateRepository(ctx, id1, virtualStorage, relativePath1, storage1, []string{storage2, storage3}, nil, false, false))
	require.NoError(t, repoStore.CreateRepository(ctx, id2, virtualStorage, relativePath2, storage1, []string{storage2, storage3}, nil, false, false))
	storageCleanup := NewStorageCleanup(db.DB)

	for _, tc := range []struct {
		desc                 string
		virtualStorage       string
		storage              string
		relativeReplicaPaths []string
		out                  []RepositoryClusterPath
	}{
		{
			desc:                 "multiple doesn't exist",
			virtualStorage:       virtualStorage,
			storage:              storage1,
			relativeReplicaPaths: []string{relativePath1, relativePath2, "path/x", "path/y"},
			out: []RepositoryClusterPath{
				{ClusterPath: ClusterPath{VirtualStorage: virtualStorage, Storage: storage1}, RelativeReplicaPath: "path/x"},
				{ClusterPath: ClusterPath{VirtualStorage: virtualStorage, Storage: storage1}, RelativeReplicaPath: "path/y"},
			},
		},
		{
			desc:                 "duplicates",
			virtualStorage:       virtualStorage,
			storage:              storage1,
			relativeReplicaPaths: []string{relativePath1, "path/x", "path/x"},
			out: []RepositoryClusterPath{
				{ClusterPath: ClusterPath{VirtualStorage: virtualStorage, Storage: storage1}, RelativeReplicaPath: "path/x"},
			},
		},
		{
			desc:                 "all exist",
			virtualStorage:       virtualStorage,
			storage:              storage1,
			relativeReplicaPaths: []string{relativePath1, relativePath2},
			out:                  nil,
		},
		{
			desc:                 "all doesn't exist",
			virtualStorage:       virtualStorage,
			storage:              storage1,
			relativeReplicaPaths: []string{"path/x", "path/y", "path/z"},
			out: []RepositoryClusterPath{
				{ClusterPath: ClusterPath{VirtualStorage: virtualStorage, Storage: storage1}, RelativeReplicaPath: "path/x"},
				{ClusterPath: ClusterPath{VirtualStorage: virtualStorage, Storage: storage1}, RelativeReplicaPath: "path/y"},
				{ClusterPath: ClusterPath{VirtualStorage: virtualStorage, Storage: storage1}, RelativeReplicaPath: "path/z"},
			},
		},
		{
			desc:                 "doesn't exist because of storage",
			virtualStorage:       virtualStorage,
			storage:              "stub",
			relativeReplicaPaths: []string{"path/x"},
			out: []RepositoryClusterPath{
				{ClusterPath: ClusterPath{VirtualStorage: virtualStorage, Storage: "stub"}, RelativeReplicaPath: "path/x"},
			},
		},
		{
			desc:                 "doesn't exist because of virtual storage",
			virtualStorage:       "stub",
			storage:              storage1,
			relativeReplicaPaths: []string{"path/x"},
			out: []RepositoryClusterPath{
				{ClusterPath: ClusterPath{VirtualStorage: "stub", Storage: storage1}, RelativeReplicaPath: "path/x"},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			res, err := storageCleanup.DoesntExist(ctx, tc.virtualStorage, tc.storage, tc.relativeReplicaPaths)
			require.NoError(t, err)
			require.ElementsMatch(t, tc.out, res)
		})
	}
}

type storageCleanupRow struct {
	ClusterPath
	LastRun     sql.NullTime
	TriggeredAt sql.NullTime
}

func getAllStoragesCleanup(t testing.TB, ctx context.Context, db glsql.Querier) []storageCleanupRow {
	rows, err := db.QueryContext(ctx, `SELECT * FROM storage_cleanups`)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	var res []storageCleanupRow
	for rows.Next() {
		var dst storageCleanupRow
		err := rows.Scan(&dst.VirtualStorage, &dst.Storage, &dst.LastRun, &dst.TriggeredAt)
		require.NoError(t, err)
		res = append(res, dst)
	}
	require.NoError(t, rows.Err())
	return res
}
