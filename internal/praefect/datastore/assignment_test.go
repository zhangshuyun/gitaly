package datastore

import (
	"sort"
	"testing"

	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/commonerr"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestAssignmentStore_GetHostAssignments(t *testing.T) {
	t.Parallel()
	type assignment struct {
		virtualStorage string
		relativePath   string
		storage        string
	}

	db := glsql.NewDB(t)

	configuredStorages := []string{"storage-1", "storage-2", "storage-3"}
	configuredVirtualStorages := map[string][]string{"virtual-storage": configuredStorages}
	for _, tc := range []struct {
		desc                      string
		configuredVirtualStorages map[string][]string
		existingAssignments       []assignment
		expectedAssignments       []string
		error                     error
	}{
		{
			desc:  "virtual storage not found",
			error: newVirtualStorageNotFoundError("virtual-storage"),
		},
		{
			desc:                      "configured storages fallback when no records",
			configuredVirtualStorages: configuredVirtualStorages,
			expectedAssignments:       configuredStorages,
		},
		{
			desc: "configured storages fallback when a repo exists in different virtual storage",
			existingAssignments: []assignment{
				{virtualStorage: "other-virtual-storage", relativePath: "relative-path", storage: "storage-1"},
			},
			configuredVirtualStorages: configuredVirtualStorages,
			expectedAssignments:       configuredStorages,
		},
		{
			desc: "configured storages fallback when a different repo exists in the virtual storage ",
			existingAssignments: []assignment{
				{virtualStorage: "virtual-storage", relativePath: "other-relative-path", storage: "storage-1"},
			},
			configuredVirtualStorages: configuredVirtualStorages,
			expectedAssignments:       configuredStorages,
		},
		{
			desc: "unconfigured storages are ignored",
			existingAssignments: []assignment{
				{virtualStorage: "virtual-storage", relativePath: "relative-path", storage: "unconfigured-storage"},
			},
			configuredVirtualStorages: configuredVirtualStorages,
			expectedAssignments:       configuredStorages,
		},
		{
			desc: "assignments found",
			existingAssignments: []assignment{
				{virtualStorage: "virtual-storage", relativePath: "relative-path", storage: "storage-1"},
				{virtualStorage: "virtual-storage", relativePath: "relative-path", storage: "storage-2"},
				{virtualStorage: "virtual-storage", relativePath: "relative-path", storage: "unconfigured"},
			},
			configuredVirtualStorages: configuredVirtualStorages,
			expectedAssignments:       []string{"storage-1", "storage-2"},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			db.TruncateAll(t)

			rs := NewPostgresRepositoryStore(db, nil)
			require.NoError(t, rs.CreateRepository(ctx, 1, "virtual-storage", "relative-path", "primary", nil, nil, false, false))
			require.NoError(t, rs.CreateRepository(ctx, 2, "virtual-storage", "other-relative-path", "primary", nil, nil, false, false))
			require.NoError(t, rs.CreateRepository(ctx, 3, "other-virtual-storage", "relative-path", "primary", nil, nil, false, false))

			for _, assignment := range tc.existingAssignments {
				repositoryID, err := rs.GetRepositoryID(ctx, assignment.virtualStorage, assignment.relativePath)
				if err != nil {
					require.Equal(t, commonerr.NewRepositoryNotFoundError(assignment.virtualStorage, assignment.relativePath), err)
				}

				_, err = db.ExecContext(ctx, `
					INSERT INTO repository_assignments (repository_id, virtual_storage, relative_path, storage)
					VALUES ($1, $2, $3, $4)
				`, repositoryID, assignment.virtualStorage, assignment.relativePath, assignment.storage)
				require.NoError(t, err)
			}

			actualAssignments, err := NewAssignmentStore(
				db,
				tc.configuredVirtualStorages,
			).GetHostAssignments(ctx, 1)
			require.Equal(t, tc.error, err)
			require.ElementsMatch(t, tc.expectedAssignments, actualAssignments)
		})
	}
}

func TestAssignmentStore_SetReplicationFactor(t *testing.T) {
	t.Parallel()
	type matcher func(testing.TB, []string)

	equal := func(expected []string) matcher {
		return func(t testing.TB, actual []string) {
			t.Helper()
			require.Equal(t, expected, actual)
		}
	}

	contains := func(expecteds ...[]string) matcher {
		return func(t testing.TB, actual []string) {
			t.Helper()
			require.Contains(t, expecteds, actual)
		}
	}

	db := glsql.NewDB(t)

	for _, tc := range []struct {
		desc                  string
		existingAssignments   []string
		nonExistentRepository bool
		replicationFactor     int
		requireStorages       matcher
		error                 error
	}{
		{
			desc:                  "increase replication factor of non-existent repository",
			nonExistentRepository: true,
			replicationFactor:     1,
			error:                 newRepositoryNotFoundError("virtual-storage", "relative-path"),
		},
		{
			desc:              "primary prioritized when setting the first assignments",
			replicationFactor: 1,
			requireStorages:   equal([]string{"primary"}),
		},
		{
			desc:                "increasing replication factor ignores unconfigured storages",
			existingAssignments: []string{"unconfigured-storage"},
			replicationFactor:   1,
			requireStorages:     equal([]string{"primary"}),
		},
		{
			desc:                "replication factor already achieved",
			existingAssignments: []string{"primary", "secondary-1"},
			replicationFactor:   2,
			requireStorages:     equal([]string{"primary", "secondary-1"}),
		},
		{
			desc:                "increase replication factor by a step",
			existingAssignments: []string{"primary"},
			replicationFactor:   2,
			requireStorages:     contains([]string{"primary", "secondary-1"}, []string{"primary", "secondary-2"}),
		},
		{
			desc:                "increase replication factor to maximum",
			existingAssignments: []string{"primary"},
			replicationFactor:   3,
			requireStorages:     equal([]string{"primary", "secondary-1", "secondary-2"}),
		},
		{
			desc:                "increased replication factor unattainable",
			existingAssignments: []string{"primary"},
			replicationFactor:   4,
			error:               newUnattainableReplicationFactorError(4, 3),
		},
		{
			desc:                "decreasing replication factor ignores unconfigured storages",
			existingAssignments: []string{"secondary-1", "unconfigured-storage"},
			replicationFactor:   1,
			requireStorages:     equal([]string{"secondary-1"}),
		},
		{
			desc:                "decrease replication factor by a step",
			existingAssignments: []string{"primary", "secondary-1", "secondary-2"},
			replicationFactor:   2,
			requireStorages:     contains([]string{"primary", "secondary-1"}, []string{"primary", "secondary-2"}),
		},
		{
			desc:                "decrease replication factor to minimum",
			existingAssignments: []string{"primary", "secondary-1", "secondary-2"},
			replicationFactor:   1,
			requireStorages:     equal([]string{"primary"}),
		},
		{
			desc:              "minimum replication factor is enforced",
			replicationFactor: 0,
			error:             newMinimumReplicationFactorError(0),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			db.TruncateAll(t)

			configuredStorages := map[string][]string{"virtual-storage": {"primary", "secondary-1", "secondary-2"}}

			if !tc.nonExistentRepository {
				_, err := db.ExecContext(ctx, `
					INSERT INTO repositories (virtual_storage, relative_path, "primary", repository_id)
					VALUES ('virtual-storage', 'relative-path', 'primary', 1)
				`)
				require.NoError(t, err)
			}

			for _, storage := range tc.existingAssignments {
				_, err := db.ExecContext(ctx, `
					INSERT INTO repository_assignments VALUES ('virtual-storage', 'relative-path', $1, 1)
				`, storage)
				require.NoError(t, err)
			}

			store := NewAssignmentStore(db, configuredStorages)

			setStorages, err := store.SetReplicationFactor(ctx, "virtual-storage", "relative-path", tc.replicationFactor)
			require.Equal(t, tc.error, err)
			if tc.error != nil {
				return
			}

			tc.requireStorages(t, setStorages)

			assignedStorages, err := store.GetHostAssignments(ctx, 1)
			require.NoError(t, err)

			sort.Strings(assignedStorages)
			tc.requireStorages(t, assignedStorages)

			var storagesWithIncorrectRepositoryID pq.StringArray
			require.NoError(t, db.QueryRowContext(ctx, `
				SELECT array_agg(storage)
				FROM repository_assignments
				WHERE COALESCE(repository_id != 1, true)
			`).Scan(&storagesWithIncorrectRepositoryID))
			require.Empty(t, storagesWithIncorrectRepositoryID)
		})
	}
}
