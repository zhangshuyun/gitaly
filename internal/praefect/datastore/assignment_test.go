// +build postgres

package datastore

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestAssignmentStore_GetHostAssignments(t *testing.T) {
	type assignment struct {
		virtualStorage string
		relativePath   string
		storage        string
	}

	configuredStorages := []string{"storage-1", "storage-2", "storage-3"}
	for _, tc := range []struct {
		desc                string
		virtualStorage      string
		existingAssignments []assignment
		expectedAssignments []string
		error               error
	}{
		{
			desc:           "virtual storage not found",
			virtualStorage: "invalid-virtual-storage",
			error:          newVirtualStorageNotFoundError("invalid-virtual-storage"),
		},
		{
			desc:                "configured storages fallback when no records",
			virtualStorage:      "virtual-storage",
			expectedAssignments: configuredStorages,
		},
		{
			desc:           "configured storages fallback when a repo exists in different virtual storage",
			virtualStorage: "virtual-storage",
			existingAssignments: []assignment{
				{virtualStorage: "other-virtual-storage", relativePath: "relative-path", storage: "storage-1"},
			},
			expectedAssignments: configuredStorages,
		},
		{
			desc:           "configured storages fallback when a different repo exists in the virtual storage ",
			virtualStorage: "virtual-storage",
			existingAssignments: []assignment{
				{virtualStorage: "virtual-storage", relativePath: "other-relative-path", storage: "storage-1"},
			},
			expectedAssignments: configuredStorages,
		},
		{
			desc:           "unconfigured storages are ignored",
			virtualStorage: "virtual-storage",
			existingAssignments: []assignment{
				{virtualStorage: "virtual-storage", relativePath: "relative-path", storage: "unconfigured-storage"},
			},
			expectedAssignments: configuredStorages,
		},
		{
			desc:           "assignments found",
			virtualStorage: "virtual-storage",
			existingAssignments: []assignment{
				{virtualStorage: "virtual-storage", relativePath: "relative-path", storage: "storage-1"},
				{virtualStorage: "virtual-storage", relativePath: "relative-path", storage: "storage-2"},
				{virtualStorage: "virtual-storage", relativePath: "relative-path", storage: "unconfigured"},
			},
			expectedAssignments: []string{"storage-1", "storage-2"},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			db := getDB(t)

			for _, assignment := range tc.existingAssignments {
				_, err := db.ExecContext(ctx, `
					INSERT INTO repositories (virtual_storage, relative_path)
					VALUES ($1, $2)
					ON CONFLICT DO NOTHING
				`, assignment.virtualStorage, assignment.relativePath)
				require.NoError(t, err)

				_, err = db.ExecContext(ctx, `
					INSERT INTO repository_assignments VALUES ($1, $2, $3)
				`, assignment.virtualStorage, assignment.relativePath, assignment.storage)
				require.NoError(t, err)
			}

			actualAssignments, err := NewAssignmentStore(
				db,
				map[string][]string{"virtual-storage": configuredStorages},
			).GetHostAssignments(ctx, tc.virtualStorage, "relative-path")
			require.Equal(t, tc.error, err)
			require.ElementsMatch(t, tc.expectedAssignments, actualAssignments)
		})
	}
}

func TestAssignmentStore_SetReplicationFactor(t *testing.T) {
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

			db := getDB(t)

			configuredStorages := map[string][]string{"virtual-storage": {"primary", "secondary-1", "secondary-2"}}

			if !tc.nonExistentRepository {
				_, err := db.ExecContext(ctx, `
					INSERT INTO repositories (virtual_storage, relative_path, "primary")
					VALUES ('virtual-storage', 'relative-path', 'primary')
				`)
				require.NoError(t, err)
			}

			for _, storage := range tc.existingAssignments {
				_, err := db.ExecContext(ctx, `
					INSERT INTO repository_assignments VALUES ('virtual-storage', 'relative-path', $1)
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

			assignedStorages, err := store.GetHostAssignments(ctx, "virtual-storage", "relative-path")
			require.NoError(t, err)
			tc.requireStorages(t, assignedStorages)
		})
	}
}
