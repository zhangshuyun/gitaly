package datastore

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestGenerationStore_Local(t *testing.T) {
	testGenerationStore(t, func(t *testing.T, storages map[string][]string) GenerationStore {
		return NewLocalGenerationStore(storages)
	})
}

func testGenerationStore(t *testing.T, newStore func(t *testing.T, storages map[string][]string) GenerationStore) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	t.Run("IncrementGeneration", func(t *testing.T) {
		gs := newStore(t, nil)

		ctx, cancel := testhelper.Context()
		defer cancel()

		t.Run("creates a new record", func(t *testing.T) {
			generation, err := gs.IncrementGeneration(ctx, "virtual-storage-1", "storage-1", "repository-1")
			require.NoError(t, err)
			require.Equal(t, 0, generation)
		})

		t.Run("increments existing record", func(t *testing.T) {
			generation, err := gs.IncrementGeneration(ctx, "virtual-storage-1", "storage-1", "repository-1")
			require.NoError(t, err)
			require.Equal(t, 1, generation)
		})
	})

	t.Run("SetGeneration", func(t *testing.T) {
		gs := newStore(t, nil)

		t.Run("creates a record", func(t *testing.T) {
			err := gs.SetGeneration(ctx, "virtual-storage-1", "storage-1", "repository-1", 1)
			require.NoError(t, err)
		})

		t.Run("updates existing record", func(t *testing.T) {
			err := gs.SetGeneration(ctx, "virtual-storage-1", "storage-1", "repository-1", 0)
			require.NoError(t, err)
		})

		t.Run("increments stays monotonic", func(t *testing.T) {
			generation, err := gs.IncrementGeneration(ctx, "virtual-storage-1", "storage-1", "repository-1")
			require.NoError(t, err)
			require.Equal(t, 2, generation)
		})
	})

	t.Run("EnsureUpgrade", func(t *testing.T) {
		t.Run("no previous record allowed", func(t *testing.T) {
			gs := newStore(t, nil)
			require.NoError(t, gs.EnsureUpgrade(ctx, "virtual-storage-1", "storage-1", "repository-1", GenerationUnknown))
			require.NoError(t, gs.EnsureUpgrade(ctx, "virtual-storage-1", "storage-1", "repository-1", 0))
		})

		t.Run("upgrade allowed", func(t *testing.T) {
			gs := newStore(t, nil)

			require.NoError(t, gs.SetGeneration(ctx, "virtual-storage-1", "storage-1", "repository-1", 0))
			require.NoError(t, gs.EnsureUpgrade(ctx, "virtual-storage-1", "storage-1", "repository-1", 1))
			require.Error(t, errDowngradeAttempted, gs.EnsureUpgrade(ctx, "virtual-storage-1", "storage-1", "repository-1", GenerationUnknown))
		})

		t.Run("downgrade prevented", func(t *testing.T) {
			gs := newStore(t, nil)

			require.NoError(t, gs.SetGeneration(ctx, "virtual-storage-1", "storage-1", "repository-1", 1))
			require.Equal(t, errDowngradeAttempted, gs.EnsureUpgrade(ctx, "virtual-storage-1", "storage-1", "repository-1", 0))
			require.Error(t, errDowngradeAttempted, gs.EnsureUpgrade(ctx, "virtual-storage-1", "storage-1", "repository-1", GenerationUnknown))
		})

		t.Run("same version prevented", func(t *testing.T) {
			gs := newStore(t, nil)

			require.NoError(t, gs.SetGeneration(ctx, "virtual-storage-1", "storage-1", "repository-1", 1))
			require.Equal(t, errDowngradeAttempted, gs.EnsureUpgrade(ctx, "virtual-storage-1", "storage-1", "repository-1", 1))
			require.Error(t, errDowngradeAttempted, gs.EnsureUpgrade(ctx, "virtual-storage-1", "storage-1", "repository-1", GenerationUnknown))
		})
	})

	t.Run("DeleteRecord", func(t *testing.T) {
		gs := newStore(t, nil)

		t.Run("delete non-existing", func(t *testing.T) {
			err := gs.DeleteRecord(ctx, "virtual-storage-1", "storage-1", "repository-1")
			require.NoError(t, err)
		})

		t.Run("delete existing", func(t *testing.T) {
			generation, err := gs.IncrementGeneration(ctx, "virtual-storage-1", "storage-1", "repository-1")
			require.NoError(t, err)
			require.Equal(t, 0, generation)

			err = gs.DeleteRecord(ctx, "virtual-storage-1", "storage-1", "repository-1")
			require.NoError(t, err)

			generation, err = gs.IncrementGeneration(ctx, "virtual-storage-1", "storage-1", "repository-1")
			require.NoError(t, err)
			require.Equal(t, 1, generation)
		})
	})

	t.Run("GetOutdatedRepositories", func(t *testing.T) {
		type state map[string]map[string]map[string]struct {
			generation int
		}

		type expected map[string]map[string]int

		for _, tc := range []struct {
			desc     string
			state    state
			expected map[string]map[string]int
		}{
			{
				"no records in virtual storage",
				state{"virtual-storage-2": {"storage-1": {"repo-1": {generation: 0}}}},
				expected{},
			},
			{
				"storages missing records",
				state{"virtual-storage-1": {"storage-1": {"repo-1": {generation: 0}}}},
				expected{"repo-1": {"storage-2": 1, "storage-3": 1}},
			},
			{
				"outdated storages",
				state{"virtual-storage-1": {
					"storage-1": {"repo-1": {generation: 2}},
					"storage-2": {"repo-1": {generation: 1}},
					"storage-3": {"repo-1": {generation: 0}},
				}},
				expected{"repo-1": {"storage-2": 1, "storage-3": 2}},
			},
			{
				"all up to date",
				state{"virtual-storage-1": {
					"storage-1": {"repo-1": {generation: 3}},
					"storage-2": {"repo-1": {generation: 3}},
					"storage-3": {"repo-1": {generation: 3}},
				}},
				expected{},
			},
		} {
			t.Run(tc.desc, func(t *testing.T) {
				gs := newStore(t, map[string][]string{"virtual-storage-1": {"storage-1", "storage-2", "storage-3"}})

				ctx, cancel := testhelper.Context()
				defer cancel()

				for vs, storages := range tc.state {
					for storage, repos := range storages {
						for repo, state := range repos {
							require.NoError(t, gs.SetGeneration(ctx, vs, storage, repo, state.generation))
						}
					}
				}

				outdated, err := gs.GetOutdatedRepositories(ctx, "virtual-storage-1")
				require.NoError(t, err)
				require.Equal(t, tc.expected, outdated)
			})
		}
	})
}
