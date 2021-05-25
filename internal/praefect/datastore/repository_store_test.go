// +build postgres

package datastore

import (
	"context"
	"database/sql"
	"testing"

	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/commonerr"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

// repositoryRecord represents Praefect's records related to a repository.
type repositoryRecord struct {
	primary     string
	assignments []string
}

// virtualStorageStates represents the virtual storage's view of which repositories should exist.
// It's structured as virtual-storage->relative_path.
type virtualStorageState map[string]map[string]repositoryRecord

// storageState contains individual storage's repository states.
// It structured as virtual-storage->relative_path->storage->generation.
type storageState map[string]map[string]map[string]int

type requireState func(t *testing.T, ctx context.Context, vss virtualStorageState, ss storageState)
type repositoryStoreFactory func(t *testing.T, storages map[string][]string) (RepositoryStore, requireState)

func TestRepositoryStore_Postgres(t *testing.T) {
	testRepositoryStore(t, func(t *testing.T, storages map[string][]string) (RepositoryStore, requireState) {
		db := getDB(t)
		gs := NewPostgresRepositoryStore(db, storages)

		requireVirtualStorageState := func(t *testing.T, ctx context.Context, exp virtualStorageState) {
			rows, err := db.QueryContext(ctx, `
SELECT virtual_storage, relative_path, "primary", assigned_storages
FROM repositories
LEFT JOIN (
	SELECT virtual_storage, relative_path, array_agg(storage ORDER BY storage) AS assigned_storages
	FROM repository_assignments
	GROUP BY virtual_storage, relative_path
) AS repository_assignments USING (virtual_storage, relative_path)

				`)
			require.NoError(t, err)
			defer rows.Close()

			act := make(virtualStorageState)
			for rows.Next() {
				var (
					virtualStorage, relativePath string
					primary                      sql.NullString
					assignments                  pq.StringArray
				)
				require.NoError(t, rows.Scan(&virtualStorage, &relativePath, &primary, &assignments))
				if act[virtualStorage] == nil {
					act[virtualStorage] = make(map[string]repositoryRecord)
				}

				act[virtualStorage][relativePath] = repositoryRecord{
					primary:     primary.String,
					assignments: assignments,
				}
			}

			require.NoError(t, rows.Err())
			require.Equal(t, exp, act)
		}

		requireStorageState := func(t *testing.T, ctx context.Context, exp storageState) {
			rows, err := db.QueryContext(ctx, `
SELECT virtual_storage, relative_path, storage, generation
FROM storage_repositories
	`)
			require.NoError(t, err)
			defer rows.Close()

			act := make(storageState)
			for rows.Next() {
				var vs, rel, storage string
				var gen int
				require.NoError(t, rows.Scan(&vs, &rel, &storage, &gen))

				if act[vs] == nil {
					act[vs] = make(map[string]map[string]int)
				}
				if act[vs][rel] == nil {
					act[vs][rel] = make(map[string]int)
				}

				act[vs][rel][storage] = gen
			}

			require.NoError(t, rows.Err())
			require.Equal(t, exp, act)
		}

		return gs, func(t *testing.T, ctx context.Context, vss virtualStorageState, ss storageState) {
			t.Helper()
			requireVirtualStorageState(t, ctx, vss)
			requireStorageState(t, ctx, ss)
		}
	})
}

func testRepositoryStore(t *testing.T, newStore repositoryStoreFactory) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	const (
		vs   = "virtual-storage-1"
		repo = "repository-1"
		stor = "storage-1"
	)

	t.Run("IncrementGeneration", func(t *testing.T) {
		t.Run("creates a new record for primary", func(t *testing.T) {
			rs, requireState := newStore(t, nil)

			require.NoError(t, rs.IncrementGeneration(ctx, vs, repo, "primary", []string{"secondary-1"}))
			requireState(t, ctx,
				virtualStorageState{
					"virtual-storage-1": {
						"repository-1": repositoryRecord{},
					},
				},
				storageState{
					"virtual-storage-1": {
						"repository-1": {
							"primary": 0,
						},
					},
				},
			)
		})

		t.Run("increments existing record for primary", func(t *testing.T) {
			rs, requireState := newStore(t, nil)

			require.NoError(t, rs.SetGeneration(ctx, vs, repo, "primary", 0))
			require.NoError(t, rs.IncrementGeneration(ctx, vs, repo, "primary", []string{"secondary-1"}))
			requireState(t, ctx,
				virtualStorageState{
					"virtual-storage-1": {
						"repository-1": repositoryRecord{},
					},
				},
				storageState{
					"virtual-storage-1": {
						"repository-1": {
							"primary": 1,
						},
					},
				},
			)
		})

		t.Run("increments existing for up to date secondary", func(t *testing.T) {
			rs, requireState := newStore(t, nil)

			require.NoError(t, rs.SetGeneration(ctx, vs, repo, "primary", 1))
			require.NoError(t, rs.SetGeneration(ctx, vs, repo, "up-to-date-secondary", 1))
			require.NoError(t, rs.SetGeneration(ctx, vs, repo, "outdated-secondary", 0))
			requireState(t, ctx,
				virtualStorageState{
					"virtual-storage-1": {
						"repository-1": repositoryRecord{},
					},
				},
				storageState{
					"virtual-storage-1": {
						"repository-1": {
							"primary":              1,
							"up-to-date-secondary": 1,
							"outdated-secondary":   0,
						},
					},
				},
			)

			require.NoError(t, rs.IncrementGeneration(ctx, vs, repo, "primary", []string{
				"up-to-date-secondary", "outdated-secondary", "non-existing-secondary"}))
			requireState(t, ctx,
				virtualStorageState{
					"virtual-storage-1": {
						"repository-1": repositoryRecord{},
					},
				},
				storageState{
					"virtual-storage-1": {
						"repository-1": {
							"primary":              2,
							"up-to-date-secondary": 2,
							"outdated-secondary":   0,
						},
					},
				},
			)
		})
	})

	t.Run("SetGeneration", func(t *testing.T) {
		t.Run("creates a record", func(t *testing.T) {
			rs, requireState := newStore(t, nil)

			err := rs.SetGeneration(ctx, vs, repo, stor, 1)
			require.NoError(t, err)
			requireState(t, ctx,
				virtualStorageState{
					"virtual-storage-1": {
						"repository-1": repositoryRecord{},
					},
				},
				storageState{
					"virtual-storage-1": {
						"repository-1": {
							"storage-1": 1,
						},
					},
				},
			)
		})

		t.Run("updates existing record", func(t *testing.T) {
			rs, requireState := newStore(t, nil)

			require.NoError(t, rs.SetGeneration(ctx, vs, repo, stor, 1))
			require.NoError(t, rs.SetGeneration(ctx, vs, repo, stor, 0))
			requireState(t, ctx,
				virtualStorageState{
					"virtual-storage-1": {
						"repository-1": repositoryRecord{},
					},
				},
				storageState{
					"virtual-storage-1": {
						"repository-1": {
							"storage-1": 0,
						},
					},
				},
			)
		})
	})

	t.Run("GetGeneration", func(t *testing.T) {
		rs, _ := newStore(t, nil)

		generation, err := rs.GetGeneration(ctx, vs, repo, stor)
		require.NoError(t, err)
		require.Equal(t, GenerationUnknown, generation)

		require.NoError(t, rs.SetGeneration(ctx, vs, repo, stor, 0))

		generation, err = rs.GetGeneration(ctx, vs, repo, stor)
		require.NoError(t, err)
		require.Equal(t, 0, generation)
	})

	t.Run("GetReplicatedGeneration", func(t *testing.T) {
		t.Run("no previous record allowed", func(t *testing.T) {
			rs, _ := newStore(t, nil)

			gen, err := rs.GetReplicatedGeneration(ctx, vs, repo, "source", "target")
			require.NoError(t, err)
			require.Equal(t, GenerationUnknown, gen)

			require.NoError(t, rs.SetGeneration(ctx, vs, repo, "source", 0))
			gen, err = rs.GetReplicatedGeneration(ctx, vs, repo, "source", "target")
			require.NoError(t, err)
			require.Equal(t, 0, gen)
		})

		t.Run("upgrade allowed", func(t *testing.T) {
			rs, _ := newStore(t, nil)

			require.NoError(t, rs.SetGeneration(ctx, vs, repo, "source", 1))
			gen, err := rs.GetReplicatedGeneration(ctx, vs, repo, "source", "target")
			require.NoError(t, err)
			require.Equal(t, 1, gen)

			require.NoError(t, rs.SetGeneration(ctx, vs, repo, "target", 0))
			gen, err = rs.GetReplicatedGeneration(ctx, vs, repo, "source", "target")
			require.NoError(t, err)
			require.Equal(t, 1, gen)
		})

		t.Run("downgrade prevented", func(t *testing.T) {
			rs, _ := newStore(t, nil)

			require.NoError(t, rs.SetGeneration(ctx, vs, repo, "target", 1))

			_, err := rs.GetReplicatedGeneration(ctx, vs, repo, "source", "target")
			require.Equal(t, DowngradeAttemptedError{vs, repo, "target", 1, GenerationUnknown}, err)

			require.NoError(t, rs.SetGeneration(ctx, vs, repo, "source", 1))
			_, err = rs.GetReplicatedGeneration(ctx, vs, repo, "source", "target")
			require.Equal(t, DowngradeAttemptedError{vs, repo, "target", 1, 1}, err)

			require.NoError(t, rs.SetGeneration(ctx, vs, repo, "source", 0))
			_, err = rs.GetReplicatedGeneration(ctx, vs, repo, "source", "target")
			require.Equal(t, DowngradeAttemptedError{vs, repo, "target", 1, 0}, err)
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
					rs, requireState := newStore(t, nil)

					require.NoError(t, rs.CreateRepository(ctx, vs, repo, "primary", tc.updatedSecondaries, tc.outdatedSecondaries, tc.storePrimary, tc.storeAssignments))

					expectedStorageState := storageState{
						vs: {
							repo: {
								"primary": 0,
							},
						},
					}

					for _, updatedSecondary := range tc.updatedSecondaries {
						expectedStorageState[vs][repo][updatedSecondary] = 0
					}

					requireState(t, ctx,
						virtualStorageState{
							vs: {
								repo: repositoryRecord{
									primary:     tc.expectedPrimary,
									assignments: tc.expectedAssignments,
								},
							},
						},
						expectedStorageState,
					)
				})
			}
		})

		t.Run("conflict", func(t *testing.T) {
			rs, _ := newStore(t, nil)

			require.NoError(t, rs.CreateRepository(ctx, vs, repo, stor, nil, nil, false, false))
			require.Equal(t,
				RepositoryExistsError{vs, repo, stor},
				rs.CreateRepository(ctx, vs, repo, stor, nil, nil, false, false),
			)
		})
	})

	t.Run("DeleteRepository", func(t *testing.T) {
		t.Run("delete non-existing", func(t *testing.T) {
			rs, _ := newStore(t, nil)

			require.Equal(t,
				RepositoryNotExistsError{vs, repo, stor},
				rs.DeleteRepository(ctx, vs, repo, stor),
			)
		})

		t.Run("delete existing", func(t *testing.T) {
			rs, requireState := newStore(t, nil)

			require.NoError(t, rs.SetGeneration(ctx, "deleted", "deleted", "deleted", 0))

			require.NoError(t, rs.SetGeneration(ctx, "virtual-storage-1", "other-storages-remain", "deleted-storage", 0))
			require.NoError(t, rs.SetGeneration(ctx, "virtual-storage-1", "other-storages-remain", "remaining-storage", 0))

			require.NoError(t, rs.SetGeneration(ctx, "virtual-storage-2", "deleted-repo", "deleted-storage", 0))
			require.NoError(t, rs.SetGeneration(ctx, "virtual-storage-2", "other-repo-remains", "remaining-storage", 0))

			requireState(t, ctx,
				virtualStorageState{
					"deleted": {
						"deleted": repositoryRecord{},
					},
					"virtual-storage-1": {
						"other-storages-remain": repositoryRecord{},
					},
					"virtual-storage-2": {
						"deleted-repo":       repositoryRecord{},
						"other-repo-remains": repositoryRecord{},
					},
				},
				storageState{
					"deleted": {
						"deleted": {
							"deleted": 0,
						},
					},
					"virtual-storage-1": {
						"other-storages-remain": {
							"deleted-storage":   0,
							"remaining-storage": 0,
						},
					},
					"virtual-storage-2": {
						"deleted-repo": {
							"deleted-storage": 0,
						},
						"other-repo-remains": {
							"remaining-storage": 0,
						},
					},
				},
			)

			require.NoError(t, rs.DeleteRepository(ctx, "deleted", "deleted", "deleted"))
			require.NoError(t, rs.DeleteRepository(ctx, "virtual-storage-1", "other-storages-remain", "deleted-storage"))
			require.NoError(t, rs.DeleteRepository(ctx, "virtual-storage-2", "deleted-repo", "deleted-storage"))

			requireState(t, ctx,
				virtualStorageState{
					"virtual-storage-2": {
						"other-repo-remains": repositoryRecord{},
					},
				},
				storageState{
					"virtual-storage-1": {
						"other-storages-remain": {
							"remaining-storage": 0,
						},
					},
					"virtual-storage-2": {
						"other-repo-remains": {
							"remaining-storage": 0,
						},
					},
				},
			)
		})
	})

	t.Run("DeleteReplica", func(t *testing.T) {
		rs, requireState := newStore(t, nil)

		t.Run("delete non-existing", func(t *testing.T) {
			require.Equal(t,
				RepositoryNotExistsError{"virtual-storage-1", "relative-path-1", "storage-1"},
				rs.DeleteReplica(ctx, "virtual-storage-1", "relative-path-1", "storage-1"),
			)
		})

		t.Run("delete existing", func(t *testing.T) {
			require.NoError(t, rs.SetGeneration(ctx, "virtual-storage-1", "relative-path-1", "storage-1", 0))
			require.NoError(t, rs.SetGeneration(ctx, "virtual-storage-1", "relative-path-1", "storage-2", 0))
			require.NoError(t, rs.SetGeneration(ctx, "virtual-storage-1", "relative-path-2", "storage-1", 0))
			require.NoError(t, rs.SetGeneration(ctx, "virtual-storage-2", "relative-path-1", "storage-1", 0))

			requireState(t, ctx,
				virtualStorageState{
					"virtual-storage-1": {
						"relative-path-1": repositoryRecord{},
						"relative-path-2": repositoryRecord{},
					},
					"virtual-storage-2": {
						"relative-path-1": repositoryRecord{},
					},
				},
				storageState{
					"virtual-storage-1": {
						"relative-path-1": {
							"storage-1": 0,
							"storage-2": 0,
						},
						"relative-path-2": {
							"storage-1": 0,
						},
					},
					"virtual-storage-2": {
						"relative-path-1": {
							"storage-1": 0,
						},
					},
				},
			)

			require.NoError(t, rs.DeleteReplica(ctx, "virtual-storage-1", "relative-path-1", "storage-1"))

			requireState(t, ctx,
				virtualStorageState{
					"virtual-storage-1": {
						"relative-path-1": repositoryRecord{},
						"relative-path-2": repositoryRecord{},
					},
					"virtual-storage-2": {
						"relative-path-1": repositoryRecord{},
					},
				},
				storageState{
					"virtual-storage-1": {
						"relative-path-1": {
							"storage-2": 0,
						},
						"relative-path-2": {
							"storage-1": 0,
						},
					},
					"virtual-storage-2": {
						"relative-path-1": {
							"storage-1": 0,
						},
					},
				},
			)
		})
	})

	t.Run("RenameRepository", func(t *testing.T) {
		t.Run("rename non-existing", func(t *testing.T) {
			rs, _ := newStore(t, nil)

			require.Equal(t,
				RepositoryNotExistsError{vs, repo, stor},
				rs.RenameRepository(ctx, vs, repo, stor, "repository-2"),
			)
		})

		t.Run("rename existing", func(t *testing.T) {
			rs, requireState := newStore(t, nil)

			require.NoError(t, rs.SetGeneration(ctx, vs, "renamed-all", "storage-1", 0))
			require.NoError(t, rs.SetGeneration(ctx, vs, "renamed-some", "storage-1", 0))
			require.NoError(t, rs.SetGeneration(ctx, vs, "renamed-some", "storage-2", 0))

			requireState(t, ctx,
				virtualStorageState{
					"virtual-storage-1": {
						"renamed-all":  repositoryRecord{},
						"renamed-some": repositoryRecord{},
					},
				},
				storageState{
					"virtual-storage-1": {
						"renamed-all": {
							"storage-1": 0,
						},
						"renamed-some": {
							"storage-1": 0,
							"storage-2": 0,
						},
					},
				},
			)

			require.NoError(t, rs.RenameRepository(ctx, vs, "renamed-all", "storage-1", "renamed-all-new"))
			require.NoError(t, rs.RenameRepository(ctx, vs, "renamed-some", "storage-1", "renamed-some-new"))

			requireState(t, ctx,
				virtualStorageState{
					"virtual-storage-1": {
						"renamed-all-new":  repositoryRecord{},
						"renamed-some-new": repositoryRecord{},
					},
				},
				storageState{
					"virtual-storage-1": {
						"renamed-all-new": {
							"storage-1": 0,
						},
						"renamed-some-new": {
							"storage-1": 0,
						},
						"renamed-some": {
							"storage-2": 0,
						},
					},
				},
			)
		})
	})

	t.Run("GetConsistentStorages", func(t *testing.T) {
		rs, requireState := newStore(t, map[string][]string{
			vs: []string{"primary", "consistent-secondary", "inconsistent-secondary", "no-record"},
		})

		t.Run("no records", func(t *testing.T) {
			secondaries, err := rs.GetConsistentStorages(ctx, vs, repo)
			require.Equal(t, commonerr.NewRepositoryNotFoundError(vs, repo), err)
			require.Empty(t, secondaries)
		})

		require.NoError(t, rs.SetGeneration(ctx, vs, repo, "primary", 1))
		require.NoError(t, rs.SetGeneration(ctx, vs, repo, "consistent-secondary", 1))
		require.NoError(t, rs.SetGeneration(ctx, vs, repo, "inconsistent-secondary", 0))
		requireState(t, ctx,
			virtualStorageState{
				"virtual-storage-1": {
					"repository-1": repositoryRecord{},
				},
			},
			storageState{
				"virtual-storage-1": {
					"repository-1": {
						"primary":                1,
						"consistent-secondary":   1,
						"inconsistent-secondary": 0,
					},
				},
			},
		)

		t.Run("consistent secondary", func(t *testing.T) {
			secondaries, err := rs.GetConsistentStorages(ctx, vs, repo)
			require.NoError(t, err)
			require.Equal(t, map[string]struct{}{"primary": struct{}{}, "consistent-secondary": struct{}{}}, secondaries)
		})

		require.NoError(t, rs.SetGeneration(ctx, vs, repo, "primary", 0))

		t.Run("outdated primary", func(t *testing.T) {
			secondaries, err := rs.GetConsistentStorages(ctx, vs, repo)
			require.NoError(t, err)
			require.Equal(t, map[string]struct{}{"consistent-secondary": struct{}{}}, secondaries)
		})

		t.Run("storage with highest generation is not configured", func(t *testing.T) {
			require.NoError(t, rs.SetGeneration(ctx, vs, repo, "unknown", 2))
			require.NoError(t, rs.SetGeneration(ctx, vs, repo, "primary", 1))
			requireState(t, ctx,
				virtualStorageState{
					"virtual-storage-1": {
						"repository-1": repositoryRecord{},
					},
				},
				storageState{
					"virtual-storage-1": {
						"repository-1": {
							"unknown":                2,
							"primary":                1,
							"consistent-secondary":   1,
							"inconsistent-secondary": 0,
						},
					},
				},
			)

			secondaries, err := rs.GetConsistentStorages(ctx, vs, repo)
			require.NoError(t, err)
			require.Equal(t, map[string]struct{}{"unknown": struct{}{}}, secondaries)
		})

		t.Run("returns not found for deleted repositories", func(t *testing.T) {
			require.NoError(t, rs.DeleteRepository(ctx, vs, repo, "primary"))
			requireState(t, ctx,
				virtualStorageState{},
				storageState{
					"virtual-storage-1": {
						"repository-1": {
							"unknown":                2,
							"consistent-secondary":   1,
							"inconsistent-secondary": 0,
						},
					},
				},
			)

			secondaries, err := rs.GetConsistentStorages(ctx, vs, repo)
			require.Equal(t, commonerr.NewRepositoryNotFoundError(vs, repo), err)
			require.Empty(t, secondaries)
		})
	})

	t.Run("DeleteInvalidRepository", func(t *testing.T) {
		t.Run("only replica", func(t *testing.T) {
			rs, requireState := newStore(t, nil)
			require.NoError(t, rs.SetGeneration(ctx, vs, repo, "invalid-storage", 0))
			require.NoError(t, rs.DeleteInvalidRepository(ctx, vs, repo, "invalid-storage"))
			requireState(t, ctx, virtualStorageState{}, storageState{})
		})

		t.Run("another replica", func(t *testing.T) {
			rs, requireState := newStore(t, nil)
			require.NoError(t, rs.SetGeneration(ctx, vs, repo, "invalid-storage", 0))
			require.NoError(t, rs.SetGeneration(ctx, vs, repo, "other-storage", 0))
			require.NoError(t, rs.DeleteInvalidRepository(ctx, vs, repo, "invalid-storage"))
			requireState(t, ctx,
				virtualStorageState{
					"virtual-storage-1": {
						"repository-1": repositoryRecord{},
					},
				},
				storageState{
					"virtual-storage-1": {
						"repository-1": {
							"other-storage": 0,
						},
					},
				},
			)
		})
	})

	t.Run("RepositoryExists", func(t *testing.T) {
		rs, _ := newStore(t, nil)

		exists, err := rs.RepositoryExists(ctx, vs, repo)
		require.NoError(t, err)
		require.False(t, exists)

		require.NoError(t, rs.SetGeneration(ctx, vs, repo, stor, 0))
		exists, err = rs.RepositoryExists(ctx, vs, repo)
		require.NoError(t, err)
		require.True(t, exists)

		require.NoError(t, rs.DeleteRepository(ctx, vs, repo, stor))
		exists, err = rs.RepositoryExists(ctx, vs, repo)
		require.NoError(t, err)
		require.False(t, exists)
	})
}

func TestPostgresRepositoryStore_GetPartiallyReplicatedRepositories(t *testing.T) {
	for _, scope := range []struct {
		desc                       string
		useVirtualStoragePrimaries bool
		primary                    string
	}{
		{desc: "virtual storage primaries", useVirtualStoragePrimaries: true, primary: "virtual-storage-primary"},
		{desc: "repository primaries", useVirtualStoragePrimaries: false, primary: "repository-primary"},
	} {
		t.Run(scope.desc, func(t *testing.T) {
			for _, tc := range []struct {
				desc                  string
				nonExistentRepository bool
				existingGenerations   map[string]int
				existingAssignments   []string
				storageDetails        []OutdatedRepositoryStorageDetails
			}{
				{
					desc:                "all up to date without assignments",
					existingGenerations: map[string]int{"primary": 0, "secondary-1": 0},
				},
				{
					desc:                "unconfigured node outdated without assignments",
					existingGenerations: map[string]int{"primary": 1, "secondary-1": 1, "unconfigured": 0},
				},
				{
					desc:                "unconfigured node contains the latest",
					existingGenerations: map[string]int{"primary": 0, "secondary-1": 0, "unconfigured": 1},
					storageDetails: []OutdatedRepositoryStorageDetails{
						{Name: "primary", BehindBy: 1, Assigned: true},
						{Name: "secondary-1", BehindBy: 1, Assigned: true},
						{Name: "unconfigured", BehindBy: 0, Assigned: false},
					},
				},
				{
					desc:                "node has no repository without assignments",
					existingGenerations: map[string]int{"primary": 0},
					storageDetails: []OutdatedRepositoryStorageDetails{
						{Name: "primary", BehindBy: 0, Assigned: true},
						{Name: "secondary-1", BehindBy: 1, Assigned: true},
					},
				},
				{
					desc:                "node has outdated repository without assignments",
					existingGenerations: map[string]int{"primary": 1, "secondary-1": 0},
					storageDetails: []OutdatedRepositoryStorageDetails{
						{Name: "primary", BehindBy: 0, Assigned: true},
						{Name: "secondary-1", BehindBy: 1, Assigned: true},
					},
				},
				{
					desc:                "node with no repository heavily outdated",
					existingGenerations: map[string]int{"primary": 10},
					storageDetails: []OutdatedRepositoryStorageDetails{
						{Name: "primary", BehindBy: 0, Assigned: true},
						{Name: "secondary-1", BehindBy: 11, Assigned: true},
					},
				},
				{
					desc:                "node with a heavily outdated repository",
					existingGenerations: map[string]int{"primary": 10, "secondary-1": 0},
					storageDetails: []OutdatedRepositoryStorageDetails{
						{Name: "primary", BehindBy: 0, Assigned: true},
						{Name: "secondary-1", BehindBy: 10, Assigned: true},
					},
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
				},
				{
					desc:                "unassigned node has an outdated repository",
					existingAssignments: []string{"primary"},
					existingGenerations: map[string]int{"primary": 1, "secondary-1": 0},
				},
				{
					desc:                "assigned node has no repository",
					existingAssignments: []string{"primary", "secondary-1"},
					existingGenerations: map[string]int{"primary": 0},
					storageDetails: []OutdatedRepositoryStorageDetails{
						{Name: "primary", BehindBy: 0, Assigned: true},
						{Name: "secondary-1", BehindBy: 1, Assigned: true},
					},
				},
				{
					desc:                "assigned node has outdated repository",
					existingAssignments: []string{"primary", "secondary-1"},
					existingGenerations: map[string]int{"primary": 1, "secondary-1": 0},
					storageDetails: []OutdatedRepositoryStorageDetails{
						{Name: "primary", BehindBy: 0, Assigned: true},
						{Name: "secondary-1", BehindBy: 1, Assigned: true},
					},
				},
				{
					desc:                "unassigned node contains the latest repository",
					existingAssignments: []string{"primary"},
					existingGenerations: map[string]int{"primary": 0, "secondary-1": 1},
					storageDetails: []OutdatedRepositoryStorageDetails{
						{Name: "primary", BehindBy: 1, Assigned: true},
						{Name: "secondary-1", BehindBy: 0, Assigned: false},
					},
				},
				{
					desc:                "unassigned node contains the only repository",
					existingAssignments: []string{"primary"},
					existingGenerations: map[string]int{"secondary-1": 0},
					storageDetails: []OutdatedRepositoryStorageDetails{
						{Name: "primary", BehindBy: 1, Assigned: true},
						{Name: "secondary-1", BehindBy: 0, Assigned: false},
					},
				},
				{
					desc:                "unassigned unconfigured node contains the only repository",
					existingAssignments: []string{"primary"},
					existingGenerations: map[string]int{"unconfigured": 0},
					storageDetails: []OutdatedRepositoryStorageDetails{
						{Name: "primary", BehindBy: 1, Assigned: true},
						{Name: "unconfigured", BehindBy: 0, Assigned: false},
					},
				},
				{
					desc:                "assigned unconfigured node has no repository",
					existingAssignments: []string{"primary", "unconfigured"},
					existingGenerations: map[string]int{"primary": 1},
				},
				{
					desc:                "assigned unconfigured node is outdated",
					existingAssignments: []string{"primary", "unconfigured"},
					existingGenerations: map[string]int{"primary": 1, "unconfigured": 0},
				},
				{
					desc:                "unconfigured node is the only assigned node",
					existingAssignments: []string{"unconfigured"},
					existingGenerations: map[string]int{"unconfigured": 0},
					storageDetails: []OutdatedRepositoryStorageDetails{
						{Name: "primary", BehindBy: 1, Assigned: true},
						{Name: "secondary-1", BehindBy: 1, Assigned: true},
						{Name: "unconfigured", BehindBy: 0, Assigned: false},
					},
				},
			} {
				t.Run(tc.desc, func(t *testing.T) {
					ctx, cancel := testhelper.Context()
					defer cancel()

					db := getDB(t)

					configuredStorages := map[string][]string{"virtual-storage": {"primary", "secondary-1"}}

					if !tc.nonExistentRepository {
						_, err := db.ExecContext(ctx, `
							INSERT INTO repositories (virtual_storage, relative_path, "primary")
							VALUES ('virtual-storage', 'relative-path', 'repository-primary')
						`)
						require.NoError(t, err)
					}

					for storage, generation := range tc.existingGenerations {
						_, err := db.ExecContext(ctx, `
							INSERT INTO storage_repositories VALUES ('virtual-storage', 'relative-path', $1, $2)
						`, storage, generation)
						require.NoError(t, err)
					}

					for _, storage := range tc.existingAssignments {
						_, err := db.ExecContext(ctx, `
							INSERT INTO repository_assignments VALUES ('virtual-storage', 'relative-path', $1)
						`, storage)
						require.NoError(t, err)
					}

					_, err := db.ExecContext(ctx, `
						INSERT INTO shard_primaries (shard_name, node_name, elected_by_praefect, elected_at)
						VALUES ('virtual-storage', 'virtual-storage-primary', 'ignored', now())
					`)
					require.NoError(t, err)

					store := NewPostgresRepositoryStore(db, configuredStorages)
					outdated, err := store.GetPartiallyReplicatedRepositories(ctx, "virtual-storage", scope.useVirtualStoragePrimaries)
					require.NoError(t, err)

					expected := []OutdatedRepository{
						{
							RelativePath: "relative-path",
							Primary:      scope.primary,
							Storages:     tc.storageDetails,
						},
					}

					if tc.storageDetails == nil {
						expected = nil
					}

					require.Equal(t, expected, outdated)
				})
			}
		})
	}
}
