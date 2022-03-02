package datastore

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/commonerr"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testdb"
)

// repositoryRecord represents Praefect's records related to a repository.
type repositoryRecord struct {
	repositoryID int64
	replicaPath  string
	primary      string
	assignments  []string
}

// virtualStorageStates represents the virtual storage's view of which repositories should exist.
// It's structured as virtual-storage->relative_path.
type virtualStorageState map[string]map[string]repositoryRecord

type replicaRecord struct {
	repositoryID int64
	generation   int
}

// storageState contains individual storage's repository states.
// It structured as virtual-storage->relative_path->storage->replicaRecord
type storageState map[string]map[string]map[string]replicaRecord

func requireState(t testing.TB, ctx context.Context, db glsql.Querier, vss virtualStorageState, ss storageState) {
	t.Helper()

	requireVirtualStorageState := func(t testing.TB, ctx context.Context, exp virtualStorageState) {
		rows, err := db.QueryContext(ctx, `
SELECT repository_id, virtual_storage, relative_path, replica_path, "primary", assigned_storages
FROM repositories
LEFT JOIN (
	SELECT repository_id, virtual_storage, relative_path, array_agg(storage ORDER BY storage) AS assigned_storages
	FROM repository_assignments
	GROUP BY repository_id, virtual_storage, relative_path
) AS repository_assignments USING (repository_id, virtual_storage, relative_path)

				`)
		require.NoError(t, err)
		defer rows.Close()

		act := make(virtualStorageState)
		for rows.Next() {
			var (
				repositoryID                              sql.NullInt64
				virtualStorage, relativePath, replicaPath string
				primary                                   sql.NullString
				assignments                               glsql.StringArray
			)
			require.NoError(t, rows.Scan(&repositoryID, &virtualStorage, &relativePath, &replicaPath, &primary, &assignments))
			if act[virtualStorage] == nil {
				act[virtualStorage] = make(map[string]repositoryRecord)
			}

			act[virtualStorage][relativePath] = repositoryRecord{
				repositoryID: repositoryID.Int64,
				replicaPath:  replicaPath,
				primary:      primary.String,
				assignments:  assignments.Slice(),
			}
		}

		require.NoError(t, rows.Err())
		require.Equal(t, exp, act)
	}

	requireStorageState := func(t testing.TB, ctx context.Context, exp storageState) {
		rows, err := db.QueryContext(ctx, `
SELECT repository_id, virtual_storage, relative_path, storage, generation
FROM storage_repositories
	`)
		require.NoError(t, err)
		defer rows.Close()

		act := make(storageState)
		for rows.Next() {
			var repositoryID sql.NullInt64
			var vs, rel, storage string
			var gen int
			require.NoError(t, rows.Scan(&repositoryID, &vs, &rel, &storage, &gen))

			if act[vs] == nil {
				act[vs] = make(map[string]map[string]replicaRecord)
			}
			if act[vs][rel] == nil {
				act[vs][rel] = make(map[string]replicaRecord)
			}

			act[vs][rel][storage] = replicaRecord{repositoryID: repositoryID.Int64, generation: gen}
		}

		require.NoError(t, rows.Err())
		require.Equal(t, exp, act)
	}

	requireVirtualStorageState(t, ctx, vss)
	requireStorageState(t, ctx, ss)
}

func TestRepositoryStore_Postgres(t *testing.T) {
	db := testdb.New(t)

	newRepositoryStore := func(t *testing.T, storages map[string][]string) RepositoryStore {
		db.TruncateAll(t)
		gs := NewPostgresRepositoryStore(db, storages)
		return gs
	}

	ctx := testhelper.Context(t)

	const (
		vs   = "virtual-storage-1"
		repo = "repository-1"
		stor = "storage-1"
	)

	t.Run("IncrementGeneration", func(t *testing.T) {
		t.Run("doesn't create new records", func(t *testing.T) {
			rs := newRepositoryStore(t, nil)

			require.Equal(t,
				rs.IncrementGeneration(ctx, 1, "primary", []string{"secondary-1"}),
				commonerr.ErrRepositoryNotFound,
			)
			requireState(t, ctx, db, virtualStorageState{}, storageState{})
		})

		t.Run("write to outdated nodes", func(t *testing.T) {
			rs := newRepositoryStore(t, nil)

			require.NoError(t, rs.CreateRepository(ctx, 1, vs, repo, "replica-path", "latest-node", []string{"outdated-primary", "outdated-secondary"}, nil, false, false))
			require.NoError(t, rs.SetGeneration(ctx, 1, "latest-node", repo, 1))

			require.Equal(t,
				rs.IncrementGeneration(ctx, 1, "outdated-primary", []string{"outdated-secondary"}),
				errWriteToOutdatedNodes,
			)
			requireState(t, ctx, db,
				virtualStorageState{
					"virtual-storage-1": {
						"repository-1": {repositoryID: 1, replicaPath: "replica-path"},
					},
				},
				storageState{
					"virtual-storage-1": {
						"repository-1": {
							"latest-node":        {repositoryID: 1, generation: 1},
							"outdated-primary":   {repositoryID: 1, generation: 0},
							"outdated-secondary": {repositoryID: 1, generation: 0},
						},
					},
				},
			)
		})

		t.Run("increments generation for up to date nodes", func(t *testing.T) {
			rs := newRepositoryStore(t, nil)

			for id, pair := range []struct{ virtualStorage, relativePath string }{
				{vs, repo},
				// create records that don't get modified to ensure the query is correctly scoped by virtual storage
				// and relative path
				{vs, "other-relative-path"},
				{"other-virtual-storage", repo},
			} {
				require.NoError(t, rs.CreateRepository(ctx, int64(id+1), pair.virtualStorage, pair.relativePath, fmt.Sprintf("replica-path-%d", id+1), "primary", []string{"up-to-date-secondary", "outdated-secondary"}, nil, false, false))
			}

			require.NoError(t, rs.IncrementGeneration(ctx, 1, "primary", []string{"up-to-date-secondary"}))

			requireState(t, ctx, db,
				virtualStorageState{
					"virtual-storage-1": {
						"repository-1":        {repositoryID: 1, replicaPath: "replica-path-1"},
						"other-relative-path": {repositoryID: 2, replicaPath: "replica-path-2"},
					},
					"other-virtual-storage": {
						"repository-1": {repositoryID: 3, replicaPath: "replica-path-3"},
					},
				},
				storageState{
					"virtual-storage-1": {
						"repository-1": {
							"primary":              {repositoryID: 1, generation: 1},
							"up-to-date-secondary": {repositoryID: 1, generation: 1},
							"outdated-secondary":   {repositoryID: 1, generation: 0},
						},
						"other-relative-path": {
							"primary":              {repositoryID: 2},
							"up-to-date-secondary": {repositoryID: 2},
							"outdated-secondary":   {repositoryID: 2},
						},
					},
					"other-virtual-storage": {
						"repository-1": {
							"primary":              {repositoryID: 3},
							"up-to-date-secondary": {repositoryID: 3},
							"outdated-secondary":   {repositoryID: 3},
						},
					},
				},
			)

			require.NoError(t, rs.IncrementGeneration(ctx, 1, "primary", []string{
				"up-to-date-secondary", "outdated-secondary", "non-existing-secondary",
			}))
			requireState(t, ctx, db,
				virtualStorageState{
					"virtual-storage-1": {
						"repository-1":        {repositoryID: 1, replicaPath: "replica-path-1"},
						"other-relative-path": {repositoryID: 2, replicaPath: "replica-path-2"},
					},
					"other-virtual-storage": {
						"repository-1": {repositoryID: 3, replicaPath: "replica-path-3"},
					},
				},
				storageState{
					"virtual-storage-1": {
						"repository-1": {
							"primary":              {repositoryID: 1, generation: 2},
							"up-to-date-secondary": {repositoryID: 1, generation: 2},
							"outdated-secondary":   {repositoryID: 1, generation: 0},
						},
						"other-relative-path": {
							"primary":              {repositoryID: 2},
							"up-to-date-secondary": {repositoryID: 2},
							"outdated-secondary":   {repositoryID: 2},
						},
					},
					"other-virtual-storage": {
						"repository-1": {
							"primary":              {repositoryID: 3},
							"up-to-date-secondary": {repositoryID: 3},
							"outdated-secondary":   {repositoryID: 3},
						},
					},
				},
			)
		})
	})

	t.Run("SetGeneration", func(t *testing.T) {
		t.Run("creates a record for the replica", func(t *testing.T) {
			rs := newRepositoryStore(t, nil)

			require.NoError(t, rs.CreateRepository(ctx, 1, vs, repo, "replica-path", stor, nil, nil, false, false))
			require.NoError(t, rs.SetGeneration(ctx, 1, "storage-2", repo, 0))
			requireState(t, ctx, db,
				virtualStorageState{"virtual-storage-1": {
					"repository-1": {repositoryID: 1, replicaPath: "replica-path"},
				}},
				storageState{
					"virtual-storage-1": {
						"repository-1": {
							"storage-1": {repositoryID: 1, generation: 0},
							"storage-2": {repositoryID: 1, generation: 0},
						},
					},
				},
			)
		})

		t.Run("updates existing record with the replicated to relative path", func(t *testing.T) {
			rs := newRepositoryStore(t, nil)

			require.NoError(t, rs.CreateRepository(ctx, 1, vs, "original-path", "replica-path", "storage-1", []string{"storage-2"}, nil, true, false))
			requireState(t, ctx, db,
				virtualStorageState{
					"virtual-storage-1": {
						"original-path": {repositoryID: 1, primary: "storage-1", replicaPath: "replica-path"},
					},
				},
				storageState{
					"virtual-storage-1": {
						"original-path": {
							"storage-2": {repositoryID: 1, generation: 0},
							"storage-1": {repositoryID: 1, generation: 0},
						},
					},
				},
			)

			require.NoError(t, rs.RenameRepository(ctx, vs, "original-path", "storage-1", "new-path"))
			requireState(t, ctx, db,
				virtualStorageState{
					"virtual-storage-1": {
						"new-path": {repositoryID: 1, primary: "storage-1", replicaPath: "new-path"},
					},
				},
				storageState{
					"virtual-storage-1": {
						"original-path": {
							"storage-2": {repositoryID: 1, generation: 0},
						},
						"new-path": {
							"storage-1": {repositoryID: 1, generation: 0},
						},
					},
				},
			)

			require.NoError(t, rs.SetGeneration(ctx, 1, "storage-2", "new-path", 1))
			requireState(t, ctx, db,
				virtualStorageState{
					"virtual-storage-1": {
						"new-path": {repositoryID: 1, primary: "storage-1", replicaPath: "new-path"},
					},
				},
				storageState{
					"virtual-storage-1": {
						"new-path": {
							"storage-1": {repositoryID: 1, generation: 0},
							"storage-2": {repositoryID: 1, generation: 1},
						},
					},
				},
			)
		})

		t.Run("updates existing record", func(t *testing.T) {
			rs := newRepositoryStore(t, nil)

			require.NoError(t, rs.CreateRepository(ctx, 1, vs, repo, "replica-path", "storage-1", nil, nil, false, false))
			require.NoError(t, rs.SetGeneration(ctx, 1, stor, repo, 1))
			require.NoError(t, rs.SetGeneration(ctx, 1, stor, repo, 0))
			requireState(t, ctx, db,
				virtualStorageState{
					"virtual-storage-1": {
						"repository-1": {repositoryID: 1, replicaPath: "replica-path"},
					},
				},
				storageState{
					"virtual-storage-1": {
						"repository-1": {
							"storage-1": {repositoryID: 1, generation: 0},
						},
					},
				},
			)
		})
	})

	t.Run("SetAuthoritativeReplica", func(t *testing.T) {
		t.Run("fails when repository doesnt exist", func(t *testing.T) {
			rs := newRepositoryStore(t, nil)

			require.Equal(t,
				commonerr.NewRepositoryNotFoundError(vs, repo),
				rs.SetAuthoritativeReplica(ctx, vs, repo, stor),
			)
		})

		t.Run("sets an existing replica as the latest", func(t *testing.T) {
			rs := newRepositoryStore(t, nil)

			require.NoError(t, rs.CreateRepository(ctx, 1, vs, repo, "replica-path", "storage-1", []string{"storage-2"}, nil, false, false))
			requireState(t, ctx, db,
				virtualStorageState{
					"virtual-storage-1": {
						"repository-1": {repositoryID: 1, replicaPath: "replica-path"},
					},
				},
				storageState{
					"virtual-storage-1": {
						"repository-1": {
							"storage-1": {repositoryID: 1, generation: 0},
							"storage-2": {repositoryID: 1, generation: 0},
						},
					},
				},
			)

			require.NoError(t, rs.SetAuthoritativeReplica(ctx, vs, repo, "storage-1"))
			requireState(t, ctx, db,
				virtualStorageState{
					"virtual-storage-1": {
						"repository-1": {repositoryID: 1, replicaPath: "replica-path"},
					},
				},
				storageState{
					"virtual-storage-1": {
						"repository-1": {
							"storage-1": {repositoryID: 1, generation: 1},
							"storage-2": {repositoryID: 1, generation: 0},
						},
					},
				},
			)
		})

		t.Run("sets a new replica as the latest", func(t *testing.T) {
			rs := newRepositoryStore(t, nil)

			require.NoError(t, rs.CreateRepository(ctx, 1, vs, repo, "replica-path", "storage-1", nil, nil, false, false))
			requireState(t, ctx, db,
				virtualStorageState{
					"virtual-storage-1": {
						"repository-1": {repositoryID: 1, replicaPath: "replica-path"},
					},
				},
				storageState{
					"virtual-storage-1": {
						"repository-1": {
							"storage-1": {repositoryID: 1, generation: 0},
						},
					},
				},
			)

			require.NoError(t, rs.SetAuthoritativeReplica(ctx, vs, repo, "storage-2"))
			requireState(t, ctx, db,
				virtualStorageState{
					"virtual-storage-1": {
						"repository-1": {repositoryID: 1, replicaPath: "replica-path"},
					},
				},
				storageState{
					"virtual-storage-1": {
						"repository-1": {
							"storage-1": {repositoryID: 1, generation: 0},
							"storage-2": {repositoryID: 1, generation: 1},
						},
					},
				},
			)
		})
	})

	t.Run("GetGeneration", func(t *testing.T) {
		rs := newRepositoryStore(t, nil)

		generation, err := rs.GetGeneration(ctx, 1, stor)
		require.NoError(t, err)
		require.Equal(t, GenerationUnknown, generation)

		require.NoError(t, rs.CreateRepository(ctx, 1, vs, repo, "replica-path", stor, nil, nil, false, false))

		generation, err = rs.GetGeneration(ctx, 1, stor)
		require.NoError(t, err)
		require.Equal(t, 0, generation)
	})

	t.Run("GetReplicatedGeneration", func(t *testing.T) {
		t.Run("no previous record allowed", func(t *testing.T) {
			rs := newRepositoryStore(t, nil)

			gen, err := rs.GetReplicatedGeneration(ctx, 1, "source", "target")
			require.NoError(t, err)
			require.Equal(t, GenerationUnknown, gen)

			require.NoError(t, rs.CreateRepository(ctx, 1, vs, repo, "replica-path", "source", nil, nil, false, false))
			gen, err = rs.GetReplicatedGeneration(ctx, 1, "source", "target")
			require.NoError(t, err)
			require.Equal(t, 0, gen)
		})

		t.Run("upgrade allowed", func(t *testing.T) {
			rs := newRepositoryStore(t, nil)

			require.NoError(t, rs.CreateRepository(ctx, 1, vs, repo, "replica-path", "source", nil, nil, false, false))
			require.NoError(t, rs.IncrementGeneration(ctx, 1, "source", nil))

			gen, err := rs.GetReplicatedGeneration(ctx, 1, "source", "target")
			require.NoError(t, err)
			require.Equal(t, 1, gen)

			require.NoError(t, rs.SetGeneration(ctx, 1, "target", repo, 0))
			gen, err = rs.GetReplicatedGeneration(ctx, 1, "source", "target")
			require.NoError(t, err)
			require.Equal(t, 1, gen)
		})

		t.Run("downgrade prevented", func(t *testing.T) {
			rs := newRepositoryStore(t, nil)

			require.NoError(t, rs.CreateRepository(ctx, 1, vs, repo, "replica-path", "target", nil, nil, false, false))
			require.NoError(t, rs.IncrementGeneration(ctx, 1, "target", nil))

			_, err := rs.GetReplicatedGeneration(ctx, 1, "source", "target")
			require.Equal(t, DowngradeAttemptedError{"target", 1, GenerationUnknown}, err)

			require.NoError(t, rs.SetGeneration(ctx, 1, "source", repo, 1))
			_, err = rs.GetReplicatedGeneration(ctx, 1, "source", "target")
			require.Equal(t, DowngradeAttemptedError{"target", 1, 1}, err)

			require.NoError(t, rs.SetGeneration(ctx, 1, "source", repo, 0))
			_, err = rs.GetReplicatedGeneration(ctx, 1, "source", "target")
			require.Equal(t, DowngradeAttemptedError{"target", 1, 0}, err)
		})
	})

	t.Run("CreateRepository", func(t *testing.T) {
		t.Run("successfully created", func(t *testing.T) {
			for _, tc := range []struct {
				desc                string
				updatedSecondaries  []string
				outdatedSecondaries []string
				storePrimary        bool
				storeAssignments    bool
				expectedPrimary     string
				expectedAssignments []string
			}{
				{
					desc: "store only repository record for primary",
				},
				{
					desc:                "store only repository records for primary and outdated secondaries",
					outdatedSecondaries: []string{"secondary-1", "secondary-2"},
				},
				{
					desc:               "store only repository records for primary and updated secondaries",
					updatedSecondaries: []string{"secondary-1", "secondary-2"},
				},
				{
					desc:                "primary stored",
					updatedSecondaries:  []string{"secondary-1"},
					outdatedSecondaries: []string{"secondary-2"},
					storePrimary:        true,
					expectedPrimary:     "primary",
				},
				{
					desc:                "assignments stored",
					storeAssignments:    true,
					updatedSecondaries:  []string{"secondary-1"},
					outdatedSecondaries: []string{"secondary-2"},
					expectedAssignments: []string{"primary", "secondary-1", "secondary-2"},
				},
				{
					desc:                "store primary and assignments",
					storePrimary:        true,
					storeAssignments:    true,
					updatedSecondaries:  []string{"secondary-1"},
					outdatedSecondaries: []string{"secondary-2"},
					expectedPrimary:     "primary",
					expectedAssignments: []string{"primary", "secondary-1", "secondary-2"},
				},
				{
					desc:                "store primary and no secondaries",
					storePrimary:        true,
					storeAssignments:    true,
					updatedSecondaries:  []string{},
					outdatedSecondaries: []string{},
					expectedPrimary:     "primary",
					expectedAssignments: []string{"primary"},
				},
				{
					desc:                "store primary and nil secondaries",
					storePrimary:        true,
					storeAssignments:    true,
					expectedPrimary:     "primary",
					expectedAssignments: []string{"primary"},
				},
			} {
				t.Run(tc.desc, func(t *testing.T) {
					rs := newRepositoryStore(t, nil)

					require.NoError(t, rs.CreateRepository(ctx, 1, vs, repo, "replica-path", "primary", tc.updatedSecondaries, tc.outdatedSecondaries, tc.storePrimary, tc.storeAssignments))

					expectedStorageState := storageState{
						vs: {
							repo: {
								"primary": {repositoryID: 1, generation: 0},
							},
						},
					}

					for _, updatedSecondary := range tc.updatedSecondaries {
						expectedStorageState[vs][repo][updatedSecondary] = replicaRecord{repositoryID: 1, generation: 0}
					}

					requireState(t, ctx, db,
						virtualStorageState{
							vs: {
								repo: {
									repositoryID: 1,
									replicaPath:  "replica-path",
									primary:      tc.expectedPrimary,
									assignments:  tc.expectedAssignments,
								},
							},
						},
						expectedStorageState,
					)
				})
			}
		})

		t.Run("conflict due to virtual storage and relative path", func(t *testing.T) {
			rs := newRepositoryStore(t, nil)

			require.NoError(t, rs.CreateRepository(ctx, 1, vs, repo, "replica-path", stor, nil, nil, false, false))
			require.Equal(t,
				RepositoryExistsError{vs, repo, stor},
				rs.CreateRepository(ctx, 2, vs, repo, "replica-path", stor, nil, nil, false, false),
			)
		})

		t.Run("conflict due to repository id", func(t *testing.T) {
			rs := newRepositoryStore(t, nil)

			require.NoError(t, rs.CreateRepository(ctx, 1, "virtual-storage-1", "relative-path-1", "replica-path", "storage-1", nil, nil, false, false))
			require.Equal(t,
				fmt.Errorf("repository id 1 already in use"),
				rs.CreateRepository(ctx, 1, "virtual-storage-2", "relative-path-2", "replica-path", "storage-2", nil, nil, false, false),
			)
		})
	})

	t.Run("DeleteRepository", func(t *testing.T) {
		t.Run("delete non-existing", func(t *testing.T) {
			rs := newRepositoryStore(t, nil)

			replicaPath, storages, err := rs.DeleteRepository(ctx, vs, repo)
			require.Equal(t, commonerr.NewRepositoryNotFoundError(vs, repo), err)
			require.Empty(t, replicaPath)
			require.Empty(t, storages)
		})

		t.Run("delete existing", func(t *testing.T) {
			rs := newRepositoryStore(t, nil)

			require.NoError(t, rs.CreateRepository(ctx, 1, "virtual-storage-1", "repository-1", "replica-path-1", "storage-1", nil, nil, false, false))
			require.NoError(t, rs.CreateRepository(ctx, 2, "virtual-storage-2", "repository-1", "replica-path-2", "storage-1", []string{"storage-2"}, nil, false, false))
			require.NoError(t, rs.CreateRepository(ctx, 3, "virtual-storage-2", "repository-2", "replica-path-3", "storage-1", nil, nil, false, false))

			requireState(t, ctx, db,
				virtualStorageState{
					"virtual-storage-1": {
						"repository-1": {repositoryID: 1, replicaPath: "replica-path-1"},
					},
					"virtual-storage-2": {
						"repository-1": {repositoryID: 2, replicaPath: "replica-path-2"},
						"repository-2": {repositoryID: 3, replicaPath: "replica-path-3"},
					},
				},
				storageState{
					"virtual-storage-1": {
						"repository-1": {
							"storage-1": {repositoryID: 1},
						},
					},
					"virtual-storage-2": {
						"repository-1": {
							"storage-1": {repositoryID: 2},
							"storage-2": {repositoryID: 2},
						},
						"repository-2": {
							"storage-1": {repositoryID: 3},
						},
					},
				},
			)

			replicaPath, storages, err := rs.DeleteRepository(ctx, "virtual-storage-2", "repository-1")
			require.NoError(t, err)
			require.Equal(t, "replica-path-2", replicaPath)
			require.Equal(t, []string{"storage-1", "storage-2"}, storages)

			requireState(t, ctx, db,
				virtualStorageState{
					"virtual-storage-1": {
						"repository-1": {repositoryID: 1, replicaPath: "replica-path-1"},
					},
					"virtual-storage-2": {
						"repository-2": {repositoryID: 3, replicaPath: "replica-path-3"},
					},
				},
				storageState{
					"virtual-storage-1": {
						"repository-1": {
							"storage-1": {repositoryID: 1},
						},
					},
					"virtual-storage-2": {
						"repository-2": {
							"storage-1": {repositoryID: 3},
						},
					},
				},
			)
		})
	})

	t.Run("DeleteReplica", func(t *testing.T) {
		rs := newRepositoryStore(t, nil)

		t.Run("delete non-existing", func(t *testing.T) {
			require.Equal(t, ErrNoRowsAffected, rs.DeleteReplica(ctx, 1, "storage-1"))
		})

		t.Run("delete existing", func(t *testing.T) {
			require.NoError(t, rs.CreateRepository(ctx, 1, "virtual-storage-1", "relative-path-1", "replica-path-1", "storage-1", []string{"storage-2"}, nil, false, false))
			require.NoError(t, rs.CreateRepository(ctx, 2, "virtual-storage-1", "relative-path-2", "replica-path-2", "storage-1", nil, nil, false, false))
			require.NoError(t, rs.CreateRepository(ctx, 3, "virtual-storage-2", "relative-path-1", "replica-path-3", "storage-1", nil, nil, false, false))

			requireState(t, ctx, db,
				virtualStorageState{
					"virtual-storage-1": {
						"relative-path-1": {repositoryID: 1, replicaPath: "replica-path-1"},
						"relative-path-2": {repositoryID: 2, replicaPath: "replica-path-2"},
					},
					"virtual-storage-2": {
						"relative-path-1": {repositoryID: 3, replicaPath: "replica-path-3"},
					},
				},
				storageState{
					"virtual-storage-1": {
						"relative-path-1": {
							"storage-1": {repositoryID: 1, generation: 0},
							"storage-2": {repositoryID: 1, generation: 0},
						},
						"relative-path-2": {
							"storage-1": {repositoryID: 2, generation: 0},
						},
					},
					"virtual-storage-2": {
						"relative-path-1": {
							"storage-1": {repositoryID: 3, generation: 0},
						},
					},
				},
			)

			require.NoError(t, rs.DeleteReplica(ctx, 1, "storage-1"))

			requireState(t, ctx, db,
				virtualStorageState{
					"virtual-storage-1": {
						"relative-path-1": {repositoryID: 1, replicaPath: "replica-path-1"},
						"relative-path-2": {repositoryID: 2, replicaPath: "replica-path-2"},
					},
					"virtual-storage-2": {
						"relative-path-1": {repositoryID: 3, replicaPath: "replica-path-3"},
					},
				},
				storageState{
					"virtual-storage-1": {
						"relative-path-1": {
							"storage-2": {repositoryID: 1, generation: 0},
						},
						"relative-path-2": {
							"storage-1": {repositoryID: 2, generation: 0},
						},
					},
					"virtual-storage-2": {
						"relative-path-1": {
							"storage-1": {repositoryID: 3, generation: 0},
						},
					},
				},
			)
		})
	})

	t.Run("RenameRepositoryInPlace", func(t *testing.T) {
		t.Run("rename non-existing", func(t *testing.T) {
			rs := newRepositoryStore(t, nil)

			require.Equal(t,
				commonerr.ErrRepositoryNotFound,
				rs.RenameRepositoryInPlace(ctx, vs, repo, "new-relative-path"),
			)
		})

		t.Run("destination exists", func(t *testing.T) {
			rs := newRepositoryStore(t, nil)

			require.NoError(t, rs.CreateRepository(ctx, 1, vs, "relative-path-1", "replica-path-1", "primary", nil, nil, true, false))
			require.NoError(t, rs.CreateRepository(ctx, 2, vs, "relative-path-2", "replica-path-2", "primary", nil, nil, true, false))

			require.Equal(t,
				commonerr.ErrRepositoryAlreadyExists,
				rs.RenameRepositoryInPlace(ctx, vs, "relative-path-1", "relative-path-2"),
			)
		})

		t.Run("successfully renamed", func(t *testing.T) {
			rs := newRepositoryStore(t, nil)

			require.NoError(t, rs.CreateRepository(ctx, 1, vs, "original-relative-path", "original-replica-path", "primary", nil, nil, false, false))
			require.NoError(t, rs.RenameRepositoryInPlace(ctx, vs, "original-relative-path", "renamed-relative-path"))
			requireState(t, ctx, db,
				virtualStorageState{
					vs: {
						"renamed-relative-path": {repositoryID: 1, replicaPath: "original-replica-path"},
					},
				},
				storageState{
					vs: {
						"renamed-relative-path": {
							"primary": {repositoryID: 1},
						},
					},
				},
			)
		})
	})

	t.Run("RenameRepository", func(t *testing.T) {
		t.Run("rename non-existing", func(t *testing.T) {
			rs := newRepositoryStore(t, nil)

			require.Equal(t,
				RepositoryNotExistsError{vs, repo, stor},
				rs.RenameRepository(ctx, vs, repo, stor, "repository-2"),
			)
		})

		t.Run("rename existing", func(t *testing.T) {
			rs := newRepositoryStore(t, nil)

			require.NoError(t, rs.CreateRepository(ctx, 1, vs, "renamed-all", "replica-path-1", "storage-1", nil, nil, false, false))
			require.NoError(t, rs.CreateRepository(ctx, 2, vs, "renamed-some", "replica-path-2", "storage-1", []string{"storage-2"}, nil, false, false))

			requireState(t, ctx, db,
				virtualStorageState{
					"virtual-storage-1": {
						"renamed-all":  {repositoryID: 1, replicaPath: "replica-path-1"},
						"renamed-some": {repositoryID: 2, replicaPath: "replica-path-2"},
					},
				},
				storageState{
					"virtual-storage-1": {
						"renamed-all": {
							"storage-1": {repositoryID: 1, generation: 0},
						},
						"renamed-some": {
							"storage-1": {repositoryID: 2, generation: 0},
							"storage-2": {repositoryID: 2, generation: 0},
						},
					},
				},
			)

			require.NoError(t, rs.RenameRepository(ctx, vs, "renamed-all", "storage-1", "renamed-all-new"))
			require.NoError(t, rs.RenameRepository(ctx, vs, "renamed-some", "storage-1", "renamed-some-new"))

			requireState(t, ctx, db,
				virtualStorageState{
					"virtual-storage-1": {
						"renamed-all-new":  {repositoryID: 1, replicaPath: "renamed-all-new"},
						"renamed-some-new": {repositoryID: 2, replicaPath: "renamed-some-new"},
					},
				},
				storageState{
					"virtual-storage-1": {
						"renamed-all-new": {
							"storage-1": {repositoryID: 1, generation: 0},
						},
						"renamed-some-new": {
							"storage-1": {repositoryID: 2, generation: 0},
						},
						"renamed-some": {
							"storage-2": {repositoryID: 2, generation: 0},
						},
					},
				},
			)
		})
	})

	t.Run("GetConsistentStorages", func(t *testing.T) {
		rs := newRepositoryStore(t, map[string][]string{
			vs: {"primary", "consistent-secondary", "inconsistent-secondary", "no-record"},
		})

		t.Run("no records", func(t *testing.T) {
			replicaPath, secondaries, err := rs.GetConsistentStorages(ctx, vs, repo)
			require.Equal(t, commonerr.NewRepositoryNotFoundError(vs, repo), err)
			require.Empty(t, replicaPath)
			require.Empty(t, secondaries)

			replicaPath, secondaries, err = rs.GetConsistentStoragesByRepositoryID(ctx, 1)
			require.Equal(t, commonerr.ErrRepositoryNotFound, err)
			require.Empty(t, replicaPath)
			require.Empty(t, secondaries)
		})

		require.NoError(t, rs.CreateRepository(ctx, 1, vs, repo, "replica-path", "primary", []string{"consistent-secondary"}, nil, false, false))
		require.NoError(t, rs.IncrementGeneration(ctx, 1, "primary", []string{"consistent-secondary"}))
		require.NoError(t, rs.SetGeneration(ctx, 1, "inconsistent-secondary", repo, 0))
		requireState(t, ctx, db,
			virtualStorageState{
				"virtual-storage-1": {
					"repository-1": {repositoryID: 1, replicaPath: "replica-path"},
				},
			},
			storageState{
				"virtual-storage-1": {
					"repository-1": {
						"primary":                {repositoryID: 1, generation: 1},
						"consistent-secondary":   {repositoryID: 1, generation: 1},
						"inconsistent-secondary": {repositoryID: 1, generation: 0},
					},
				},
			},
		)

		t.Run("consistent secondary", func(t *testing.T) {
			replicaPath, secondaries, err := rs.GetConsistentStorages(ctx, vs, repo)
			require.NoError(t, err)
			require.Equal(t, map[string]struct{}{"primary": {}, "consistent-secondary": {}}, secondaries)
			require.Equal(t, "replica-path", replicaPath)

			replicaPath, secondaries, err = rs.GetConsistentStoragesByRepositoryID(ctx, 1)
			require.NoError(t, err)
			require.Equal(t, map[string]struct{}{"primary": {}, "consistent-secondary": {}}, secondaries)
			require.Equal(t, "replica-path", replicaPath)
		})

		require.NoError(t, rs.SetGeneration(ctx, 1, "primary", repo, 0))

		t.Run("outdated primary", func(t *testing.T) {
			replicaPath, secondaries, err := rs.GetConsistentStorages(ctx, vs, repo)
			require.NoError(t, err)
			require.Equal(t, map[string]struct{}{"consistent-secondary": {}}, secondaries)
			require.Equal(t, "replica-path", replicaPath)

			replicaPath, secondaries, err = rs.GetConsistentStoragesByRepositoryID(ctx, 1)
			require.NoError(t, err)
			require.Equal(t, map[string]struct{}{"consistent-secondary": {}}, secondaries)
			require.Equal(t, "replica-path", replicaPath)
		})

		t.Run("storage with highest generation is not configured", func(t *testing.T) {
			require.NoError(t, rs.SetGeneration(ctx, 1, "unknown", repo, 2))
			require.NoError(t, rs.SetGeneration(ctx, 1, "primary", repo, 1))
			requireState(t, ctx, db,
				virtualStorageState{
					"virtual-storage-1": {
						"repository-1": {repositoryID: 1, replicaPath: "replica-path"},
					},
				},
				storageState{
					"virtual-storage-1": {
						"repository-1": {
							"unknown":                {repositoryID: 1, generation: 2},
							"primary":                {repositoryID: 1, generation: 1},
							"consistent-secondary":   {repositoryID: 1, generation: 1},
							"inconsistent-secondary": {repositoryID: 1, generation: 0},
						},
					},
				},
			)

			replicaPath, secondaries, err := rs.GetConsistentStorages(ctx, vs, repo)
			require.NoError(t, err)
			require.Equal(t, map[string]struct{}{"unknown": {}}, secondaries)
			require.Equal(t, "replica-path", replicaPath)

			replicaPath, secondaries, err = rs.GetConsistentStoragesByRepositoryID(ctx, 1)
			require.NoError(t, err)
			require.Equal(t, map[string]struct{}{"unknown": {}}, secondaries)
			require.Equal(t, "replica-path", replicaPath)
		})

		t.Run("returns not found for deleted repositories", func(t *testing.T) {
			_, _, err := rs.DeleteRepository(ctx, vs, repo)
			require.NoError(t, err)
			requireState(t, ctx, db, virtualStorageState{}, storageState{})

			replicaPath, secondaries, err := rs.GetConsistentStorages(ctx, vs, repo)
			require.Equal(t, commonerr.NewRepositoryNotFoundError(vs, repo), err)
			require.Empty(t, secondaries)
			require.Empty(t, replicaPath)

			replicaPath, secondaries, err = rs.GetConsistentStoragesByRepositoryID(ctx, 1)
			require.Equal(t, commonerr.ErrRepositoryNotFound, err)
			require.Empty(t, secondaries)
			require.Empty(t, replicaPath)
		})

		t.Run("replicas pending rename are considered outdated", func(t *testing.T) {
			rs := newRepositoryStore(t, nil)

			require.NoError(t, rs.CreateRepository(ctx, 1, vs, "original-path", "replica-path", "storage-1", []string{"storage-2"}, nil, true, false))
			replicaPath, storages, err := rs.GetConsistentStorages(ctx, vs, "original-path")
			require.NoError(t, err)
			require.Equal(t, "replica-path", replicaPath)
			require.Equal(t, map[string]struct{}{"storage-1": {}, "storage-2": {}}, storages)
			replicaPath, storages, err = rs.GetConsistentStoragesByRepositoryID(ctx, 1)
			require.NoError(t, err)
			require.Equal(t, "replica-path", replicaPath)
			require.Equal(t, map[string]struct{}{"storage-1": {}, "storage-2": {}}, storages)

			require.NoError(t, rs.RenameRepository(ctx, vs, "original-path", "storage-1", "new-path"))
			replicaPath, storages, err = rs.GetConsistentStorages(ctx, vs, "new-path")
			require.NoError(t, err)
			require.Equal(t, "new-path", replicaPath)
			require.Equal(t, map[string]struct{}{"storage-1": {}}, storages)
			replicaPath, storages, err = rs.GetConsistentStoragesByRepositoryID(ctx, 1)
			require.NoError(t, err)
			require.Equal(t, "new-path", replicaPath)
			require.Equal(t, map[string]struct{}{"storage-1": {}}, storages)

			require.NoError(t, rs.RenameRepository(ctx, vs, "original-path", "storage-2", "new-path"))
			replicaPath, storages, err = rs.GetConsistentStorages(ctx, vs, "new-path")
			require.NoError(t, err)
			require.Equal(t, "new-path", replicaPath)
			require.Equal(t, map[string]struct{}{"storage-1": {}, "storage-2": {}}, storages)
			replicaPath, storages, err = rs.GetConsistentStoragesByRepositoryID(ctx, 1)
			require.NoError(t, err)
			require.Equal(t, "new-path", replicaPath)
			require.Equal(t, map[string]struct{}{"storage-1": {}, "storage-2": {}}, storages)
		})
	})

	t.Run("DeleteInvalidRepository", func(t *testing.T) {
		t.Run("only replica", func(t *testing.T) {
			rs := newRepositoryStore(t, nil)
			require.NoError(t, rs.CreateRepository(ctx, 1, vs, repo, "replica-path", "invalid-storage", nil, nil, false, false))
			require.NoError(t, rs.DeleteInvalidRepository(ctx, 1, "invalid-storage"))
			requireState(t, ctx, db, virtualStorageState{}, storageState{})
		})

		t.Run("another replica", func(t *testing.T) {
			rs := newRepositoryStore(t, nil)
			require.NoError(t, rs.CreateRepository(ctx, 1, vs, repo, "replica-path", "invalid-storage", []string{"other-storage"}, nil, false, false))
			require.NoError(t, rs.DeleteInvalidRepository(ctx, 1, "invalid-storage"))
			requireState(t, ctx, db,
				virtualStorageState{
					"virtual-storage-1": {
						"repository-1": {repositoryID: 1, replicaPath: "replica-path"},
					},
				},
				storageState{
					"virtual-storage-1": {
						"repository-1": {
							"other-storage": {repositoryID: 1, generation: 0},
						},
					},
				},
			)
		})
	})

	t.Run("RepositoryExists", func(t *testing.T) {
		rs := newRepositoryStore(t, nil)

		exists, err := rs.RepositoryExists(ctx, vs, repo)
		require.NoError(t, err)
		require.False(t, exists)

		require.NoError(t, rs.CreateRepository(ctx, 1, vs, repo, "replica-path", stor, nil, nil, false, false))
		exists, err = rs.RepositoryExists(ctx, vs, repo)
		require.NoError(t, err)
		require.True(t, exists)

		_, _, err = rs.DeleteRepository(ctx, vs, repo)
		require.NoError(t, err)
		exists, err = rs.RepositoryExists(ctx, vs, repo)
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("ReserveRepositoryID", func(t *testing.T) {
		rs := newRepositoryStore(t, nil)

		id, err := rs.ReserveRepositoryID(ctx, vs, repo)
		require.NoError(t, err)
		require.Equal(t, int64(1), id)

		id, err = rs.ReserveRepositoryID(ctx, vs, repo)
		require.NoError(t, err)
		require.Equal(t, int64(2), id)

		require.NoError(t, rs.CreateRepository(ctx, id, vs, repo, "replica-path", stor, nil, nil, false, false))

		id, err = rs.ReserveRepositoryID(ctx, vs, repo)
		require.Equal(t, commonerr.ErrRepositoryAlreadyExists, err)
		require.Equal(t, int64(0), id)

		id, err = rs.ReserveRepositoryID(ctx, vs, repo+"-2")
		require.NoError(t, err)
		require.Equal(t, int64(3), id)
	})

	t.Run("GetRepositoryID", func(t *testing.T) {
		rs := newRepositoryStore(t, nil)

		id, err := rs.GetRepositoryID(ctx, vs, repo)
		require.Equal(t, commonerr.NewRepositoryNotFoundError(vs, repo), err)
		require.Equal(t, int64(0), id)

		require.NoError(t, rs.CreateRepository(ctx, 1, vs, repo, "replica-path", stor, nil, nil, false, false))

		id, err = rs.GetRepositoryID(ctx, vs, repo)
		require.Nil(t, err)
		require.Equal(t, int64(1), id)
	})

	t.Run("GetReplicaPath", func(t *testing.T) {
		rs := newRepositoryStore(t, nil)

		replicaPath, err := rs.GetReplicaPath(ctx, 1)
		require.Equal(t, err, commonerr.ErrRepositoryNotFound)
		require.Empty(t, replicaPath)

		require.NoError(t, rs.CreateRepository(ctx, 1, vs, repo, "replica-path", stor, nil, nil, false, false))

		replicaPath, err = rs.GetReplicaPath(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, "replica-path", replicaPath)
	})
}

func TestRepositoryStore_incrementGenerationConcurrently(t *testing.T) {
	db := testdb.New(t)

	type call struct {
		primary     string
		secondaries []string
	}

	for _, tc := range []struct {
		desc   string
		first  call
		second call
		error  error
		state  storageState
	}{
		{
			desc: "both successful",
			first: call{
				primary:     "primary",
				secondaries: []string{"secondary"},
			},
			second: call{
				primary:     "primary",
				secondaries: []string{"secondary"},
			},
			state: storageState{
				"virtual-storage": {
					"relative-path": {
						"primary":   {repositoryID: 1, generation: 2},
						"secondary": {repositoryID: 1, generation: 2},
					},
				},
			},
		},
		{
			desc: "second write targeted outdated and up to date nodes",
			first: call{
				primary: "primary",
			},
			second: call{
				primary:     "primary",
				secondaries: []string{"secondary"},
			},
			state: storageState{
				"virtual-storage": {
					"relative-path": {
						"primary":   {repositoryID: 1, generation: 2},
						"secondary": {repositoryID: 1, generation: 0},
					},
				},
			},
		},
		{
			desc: "second write targeted only outdated nodes",
			first: call{
				primary: "primary",
			},
			second: call{
				primary: "secondary",
			},
			error: errWriteToOutdatedNodes,
			state: storageState{
				"virtual-storage": {
					"relative-path": {
						"primary":   {repositoryID: 1, generation: 1},
						"secondary": {repositoryID: 1, generation: 0},
					},
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)

			db.TruncateAll(t)

			require.NoError(t, NewPostgresRepositoryStore(db, nil).CreateRepository(ctx, 1, "virtual-storage", "relative-path", "replica-path", "primary", []string{"secondary"}, nil, false, false))

			firstTx := db.Begin(t)
			secondTx := db.Begin(t)

			err := NewPostgresRepositoryStore(firstTx, nil).IncrementGeneration(ctx, 1, tc.first.primary, tc.first.secondaries)
			require.NoError(t, err)

			go func() {
				testdb.WaitForBlockedQuery(ctx, t, db, "WITH updated_replicas AS (")
				firstTx.Commit(t)
			}()

			err = NewPostgresRepositoryStore(secondTx, nil).IncrementGeneration(ctx, 1, tc.second.primary, tc.second.secondaries)
			require.Equal(t, tc.error, err)
			secondTx.Commit(t)

			requireState(t, ctx, db,
				virtualStorageState{"virtual-storage": {"relative-path": {repositoryID: 1, replicaPath: "replica-path"}}},
				tc.state,
			)
		})
	}
}

func TestPostgresRepositoryStore_GetRepositoryMetadata(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)
	for _, tc := range []struct {
		desc                  string
		nonExistentRepository bool
		unhealthyStorages     map[string]struct{}
		existingGenerations   map[string]int
		existingAssignments   []string
		replicas              []Replica

		hasPartiallyReplicated bool
	}{
		{
			desc:                "all up to date without assignments",
			existingGenerations: map[string]int{"primary": 0, "secondary-1": 0},
			replicas: []Replica{
				{Storage: "primary", Generation: 0, Assigned: true, Healthy: true, ValidPrimary: true},
				{Storage: "secondary-1", Generation: 0, Assigned: true, Healthy: true, ValidPrimary: true},
			},
		},
		{
			desc:                "unconfigured node outdated without assignments",
			existingGenerations: map[string]int{"primary": 1, "secondary-1": 1, "unconfigured": 0},
			replicas: []Replica{
				{Storage: "primary", Generation: 1, Assigned: true, Healthy: true, ValidPrimary: true},
				{Storage: "secondary-1", Generation: 1, Assigned: true, Healthy: true, ValidPrimary: true},
				{Storage: "unconfigured", Generation: 0},
			},
		},
		{
			desc:                "unconfigured node contains the latest",
			existingGenerations: map[string]int{"primary": 0, "secondary-1": 0, "unconfigured": 1},
			replicas: []Replica{
				{Storage: "primary", Generation: 0, Assigned: true, Healthy: true},
				{Storage: "secondary-1", Generation: 0, Assigned: true, Healthy: true},
				{Storage: "unconfigured", Generation: 1, Assigned: false},
			},
			hasPartiallyReplicated: true,
		},
		{
			desc:                "node has no repository without assignments",
			existingGenerations: map[string]int{"primary": 0},
			replicas: []Replica{
				{Storage: "primary", Generation: 0, Assigned: true, Healthy: true, ValidPrimary: true},
				{Storage: "secondary-1", Generation: -1, Assigned: true, Healthy: true},
			},
			hasPartiallyReplicated: true,
		},
		{
			desc:                "node has outdated repository without assignments",
			existingGenerations: map[string]int{"primary": 1, "secondary-1": 0},
			replicas: []Replica{
				{Storage: "primary", Generation: 1, Assigned: true, Healthy: true, ValidPrimary: true},
				{Storage: "secondary-1", Generation: 0, Assigned: true, Healthy: true},
			},
			hasPartiallyReplicated: true,
		},
		{
			desc:                "node with no repository heavily outdated",
			existingGenerations: map[string]int{"primary": 10},
			replicas: []Replica{
				{Storage: "primary", Generation: 10, Assigned: true, Healthy: true, ValidPrimary: true},
				{Storage: "secondary-1", Generation: -1, Assigned: true, Healthy: true},
			},
			hasPartiallyReplicated: true,
		},
		{
			desc:                "node with a heavily outdated repository",
			existingGenerations: map[string]int{"primary": 10, "secondary-1": 0},
			replicas: []Replica{
				{Storage: "primary", Generation: 10, Assigned: true, Healthy: true, ValidPrimary: true},
				{Storage: "secondary-1", Generation: 0, Assigned: true, Healthy: true},
			},
			hasPartiallyReplicated: true,
		},
		{
			desc:                  "outdated nodes ignored when repository should not exist",
			nonExistentRepository: true,
			existingGenerations:   map[string]int{"primary": 1, "secondary-1": 0},
		},
		{
			desc:                "unassigned node has no repository",
			existingAssignments: []string{"primary"},
			existingGenerations: map[string]int{"primary": 0},
			replicas: []Replica{
				{Storage: "primary", Generation: 0, Assigned: true, Healthy: true, ValidPrimary: true},
			},
		},
		{
			desc:                "unassigned node has an outdated repository",
			existingAssignments: []string{"primary"},
			existingGenerations: map[string]int{"primary": 1, "secondary-1": 0},
			replicas: []Replica{
				{Storage: "primary", Generation: 1, Assigned: true, Healthy: true, ValidPrimary: true},
				{Storage: "secondary-1", Generation: 0, Healthy: true},
			},
		},
		{
			desc:                "assigned node has no repository",
			existingAssignments: []string{"primary", "secondary-1"},
			existingGenerations: map[string]int{"primary": 0},
			replicas: []Replica{
				{Storage: "primary", Generation: 0, Assigned: true, Healthy: true, ValidPrimary: true},
				{Storage: "secondary-1", Generation: -1, Assigned: true, Healthy: true},
			},
			hasPartiallyReplicated: true,
		},
		{
			desc:                "assigned node has outdated repository",
			existingAssignments: []string{"primary", "secondary-1"},
			existingGenerations: map[string]int{"primary": 1, "secondary-1": 0},
			replicas: []Replica{
				{Storage: "primary", Generation: 1, Assigned: true, Healthy: true, ValidPrimary: true},
				{Storage: "secondary-1", Generation: 0, Assigned: true, Healthy: true},
			},
			hasPartiallyReplicated: true,
		},
		{
			desc:                "unassigned node contains the latest repository",
			existingAssignments: []string{"primary"},
			existingGenerations: map[string]int{"primary": 0, "secondary-1": 1},
			replicas: []Replica{
				{Storage: "primary", Generation: 0, Assigned: true, Healthy: true},
				{Storage: "secondary-1", Generation: 1, Assigned: false, Healthy: true, ValidPrimary: true},
			},
			hasPartiallyReplicated: true,
		},
		{
			desc:                "unassigned node contains the only repository",
			existingAssignments: []string{"primary"},
			existingGenerations: map[string]int{"secondary-1": 0},
			replicas: []Replica{
				{Storage: "primary", Generation: -1, Assigned: true, Healthy: true},
				{Storage: "secondary-1", Generation: 0, Assigned: false, Healthy: true, ValidPrimary: true},
			},
			hasPartiallyReplicated: true,
		},
		{
			desc:                "unassigned unconfigured node contains the only repository",
			existingAssignments: []string{"primary"},
			existingGenerations: map[string]int{"unconfigured": 0},
			replicas: []Replica{
				{Storage: "primary", Generation: -1, Assigned: true, Healthy: true},
				{Storage: "unconfigured", Generation: 0, Assigned: false},
			},
			hasPartiallyReplicated: true,
		},
		{
			desc:                "assigned unconfigured node has no repository",
			existingAssignments: []string{"primary", "unconfigured"},
			existingGenerations: map[string]int{"primary": 1},
			replicas: []Replica{
				{Storage: "primary", Generation: 1, Assigned: true, Healthy: true, ValidPrimary: true},
			},
		},
		{
			desc:                "assigned unconfigured node is outdated",
			existingAssignments: []string{"primary", "unconfigured"},
			existingGenerations: map[string]int{"primary": 1, "unconfigured": 0},
			replicas: []Replica{
				{Storage: "primary", Generation: 1, Assigned: true, Healthy: true, ValidPrimary: true},
				{Storage: "unconfigured", Generation: 0, Assigned: false},
			},
		},
		{
			desc:                "unconfigured node is the only assigned node",
			existingAssignments: []string{"unconfigured"},
			existingGenerations: map[string]int{"unconfigured": 0},
			replicas: []Replica{
				{Storage: "primary", Generation: -1, Assigned: true, Healthy: true},
				{Storage: "secondary-1", Generation: -1, Assigned: true, Healthy: true},
				{Storage: "unconfigured", Generation: 0, Assigned: false},
			},
			hasPartiallyReplicated: true,
		},
		{
			desc:                "repository is fully replicated but unavailable",
			unhealthyStorages:   map[string]struct{}{"primary": {}, "secondary-1": {}},
			existingAssignments: []string{"primary", "secondary-1"},
			existingGenerations: map[string]int{"primary": 0, "secondary-1": 0},
			replicas: []Replica{
				{Storage: "primary", Generation: 0, Assigned: true},
				{Storage: "secondary-1", Generation: 0, Assigned: true},
			},
			hasPartiallyReplicated: true,
		},
		{
			desc:                "assigned replicas unavailable but a valid unassigned primary candidate",
			unhealthyStorages:   map[string]struct{}{"primary": {}},
			existingAssignments: []string{"primary"},
			existingGenerations: map[string]int{"primary": 0, "secondary-1": 0},
			replicas: []Replica{
				{Storage: "primary", Generation: 0, Assigned: true},
				{Storage: "secondary-1", Generation: 0, Healthy: true, ValidPrimary: true},
			},
			hasPartiallyReplicated: true,
		},
		{
			desc:                "assigned replicas available but unassigned replica unavailable",
			unhealthyStorages:   map[string]struct{}{"secondary-1": {}},
			existingAssignments: []string{"primary"},
			existingGenerations: map[string]int{"primary": 0, "secondary-1": 0},
			replicas: []Replica{
				{Storage: "primary", Generation: 0, Assigned: true, Healthy: true, ValidPrimary: true},
				{Storage: "secondary-1", Generation: 0},
			},
		},
		{
			desc:                "one assigned replica unavailable",
			unhealthyStorages:   map[string]struct{}{"secondary-1": {}},
			existingAssignments: []string{"primary", "secondary-1"},
			existingGenerations: map[string]int{"primary": 0, "secondary-1": 0},
			replicas: []Replica{
				{Storage: "primary", Generation: 0, Assigned: true, Healthy: true, ValidPrimary: true},
				{Storage: "secondary-1", Generation: 0, Assigned: true},
			},
			hasPartiallyReplicated: true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)

			tx := db.Begin(t)
			defer tx.Rollback(t)

			configuredStorages := map[string][]string{"virtual-storage": {"primary", "secondary-1"}}

			var healthyStorages []string
			for _, storage := range configuredStorages["virtual-storage"] {
				if _, ok := tc.unhealthyStorages[storage]; ok {
					continue
				}

				healthyStorages = append(healthyStorages, storage)
			}

			testdb.SetHealthyNodes(t, ctx, tx, map[string]map[string][]string{
				"praefect-0": {"virtual-storage": healthyStorages},
			})

			const (
				virtualStorage = "virtual-storage"
				relativePath   = "relative-path"
				replicaPath    = "replica-path"
			)

			rs := NewPostgresRepositoryStore(tx, configuredStorages)

			repositoryID, err := rs.ReserveRepositoryID(ctx, virtualStorage, relativePath)
			require.NoError(t, err)

			_, err = tx.ExecContext(ctx, `
				INSERT INTO repositories (repository_id, virtual_storage, relative_path, replica_path, "primary")
				VALUES ($1, $2, $3, $4, 'repository-primary')
			`, repositoryID, virtualStorage, relativePath, replicaPath)
			require.NoError(t, err)

			maxGeneration := 0
			for storage, generation := range tc.existingGenerations {
				if maxGeneration < generation {
					maxGeneration = generation
				}

				require.NoError(t, rs.SetGeneration(ctx, repositoryID, storage, relativePath, generation))
			}

			for _, storage := range tc.existingAssignments {
				_, err := tx.ExecContext(ctx, `
					INSERT INTO repository_assignments (repository_id, virtual_storage, relative_path, storage)
					VALUES ($1, $2, $3, $4)
				`, repositoryID, virtualStorage, relativePath, storage)
				require.NoError(t, err)
			}

			if tc.nonExistentRepository {
				// The repository record should always be created anyway prior to the deletion to match the real
				// scenario. This way any foreign key cascades are also applied correctly to other records.
				_, err := tx.ExecContext(ctx, "DELETE FROM repositories WHERE repository_id = $1", repositoryID)
				require.NoError(t, err)
			}

			expectedMetadata := RepositoryMetadata{
				RepositoryID:   repositoryID,
				VirtualStorage: virtualStorage,
				RelativePath:   relativePath,
				ReplicaPath:    replicaPath,
				Primary:        "repository-primary",
				Generation:     int64(maxGeneration),
				Replicas:       tc.replicas,
			}

			expectedPartiallyReplicated := []RepositoryMetadata{expectedMetadata}
			if !tc.hasPartiallyReplicated {
				expectedPartiallyReplicated = nil
			}

			partiallyReplicated, err := rs.GetPartiallyAvailableRepositories(ctx, virtualStorage)
			require.NoError(t, err)
			require.Equal(t, expectedPartiallyReplicated, partiallyReplicated)

			var expectedErr error
			if tc.nonExistentRepository {
				expectedErr = commonerr.ErrRepositoryNotFound
				expectedMetadata = RepositoryMetadata{}
			}

			metadata, err := rs.GetRepositoryMetadata(ctx, repositoryID)
			require.Equal(t, expectedErr, err)
			require.Equal(t, expectedMetadata, metadata)

			metadata, err = rs.GetRepositoryMetadataByPath(ctx, virtualStorage, relativePath)
			require.Equal(t, expectedErr, err)
			require.Equal(t, expectedMetadata, metadata)
		})
	}
}
