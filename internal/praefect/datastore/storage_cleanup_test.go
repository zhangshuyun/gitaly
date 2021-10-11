// +build postgres

package datastore

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestStorageCleanup_Exists(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	db := getDB(t)

	repoStore := NewPostgresRepositoryStore(db.DB, nil)
	require.NoError(t, repoStore.CreateRepository(ctx, "vs", "p/1", "g1", []string{"g2", "g3"}, nil, false, false))
	require.NoError(t, repoStore.CreateRepository(ctx, "vs", "p/2", "g1", []string{"g2", "g3"}, nil, false, false))
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
			virtualStorage:       "vs",
			storage:              "g1",
			relativeReplicaPaths: []string{"p/1", "p/2", "path/x", "path/y"},
			out: []RepositoryClusterPath{
				NewRepositoryClusterPath("vs", "g1", "path/x"),
				NewRepositoryClusterPath("vs", "g1", "path/y"),
			},
		},
		{
			desc:                 "duplicates",
			virtualStorage:       "vs",
			storage:              "g1",
			relativeReplicaPaths: []string{"p/1", "path/x", "path/x"},
			out: []RepositoryClusterPath{
				NewRepositoryClusterPath("vs", "g1", "path/x"),
			},
		},
		{
			desc:                 "all exist",
			virtualStorage:       "vs",
			storage:              "g1",
			relativeReplicaPaths: []string{"p/1", "p/2"},
			out:                  nil,
		},
		{
			desc:                 "all doesn't exist",
			virtualStorage:       "vs",
			storage:              "g1",
			relativeReplicaPaths: []string{"path/x", "path/y", "path/z"},
			out: []RepositoryClusterPath{
				NewRepositoryClusterPath("vs", "g1", "path/x"),
				NewRepositoryClusterPath("vs", "g1", "path/y"),
				NewRepositoryClusterPath("vs", "g1", "path/z"),
			},
		},
		{
			desc:                 "doesn't exist because of storage",
			virtualStorage:       "vs",
			storage:              "stub",
			relativeReplicaPaths: []string{"path/x"},
			out: []RepositoryClusterPath{
				NewRepositoryClusterPath("vs", "stub", "path/x"),
			},
		},
		{
			desc:                 "doesn't exist because of virtual storage",
			virtualStorage:       "stub",
			storage:              "g1",
			relativeReplicaPaths: []string{"path/x"},
			out: []RepositoryClusterPath{
				NewRepositoryClusterPath("stub", "g1", "path/x"),
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
