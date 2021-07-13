// +build postgres

package datastore

import (
	"fmt"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestRepositoryStoreCollector(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	type replicas map[string]struct {
		generation int
		assigned   bool
	}

	type repositories []struct {
		deleted      bool
		relativePath string
		replicas     replicas
	}

	for _, tc := range []struct {
		desc         string
		healthyNodes []string
		repositories repositories
		count        int
	}{
		{
			desc: "no repositories",
		},
		{
			desc: "deleted repositories are not considered unavailable",
			repositories: repositories{
				{
					deleted:      true,
					relativePath: "repository-1",
					replicas: replicas{
						"storage-1": {generation: 0},
					},
				},
			},
		},
		{
			desc: "repositories without any healthy replicas are counted",
			repositories: repositories{
				{
					relativePath: "repository-1",
					replicas: replicas{
						"storage-1": {generation: 0},
					},
				},
			},
			count: 1,
		},
		{
			desc:         "repositories with healthy replicas are not counted",
			healthyNodes: []string{"storage-1"},
			repositories: repositories{
				{
					relativePath: "repository-1",
					replicas: replicas{
						"storage-1": {generation: 0},
					},
				},
			},
		},
		{
			desc:         "repositories with only outdated healthy replicas are counted",
			healthyNodes: []string{"storage-1"},
			repositories: repositories{
				{
					relativePath: "repository-1",
					replicas: replicas{
						"storage-1": {generation: 0},
						"storage-2": {generation: 1},
					},
				},
			},
			count: 1,
		},
		{
			desc:         "repositories with unassigned fully up to date healthy replicas are not counted",
			healthyNodes: []string{"storage-2"},
			repositories: repositories{
				{
					relativePath: "repository-1",
					replicas: replicas{
						"storage-1": {generation: 1, assigned: true},
						"storage-2": {generation: 1},
					},
				},
			},
		},
		{
			desc:         "repositories with unassigned, outdated replicas is not unavailable",
			healthyNodes: []string{"storage-1"},
			repositories: repositories{
				{
					relativePath: "repository-1",
					replicas: replicas{
						"storage-1": {generation: 1, assigned: true},
						"storage-2": {generation: 0},
					},
				},
			},
		},
		{
			desc:         "multiple unavailable repositories are counted correctly",
			healthyNodes: []string{"storage-2"},
			repositories: repositories{
				{
					relativePath: "repository-1",
					replicas: replicas{
						"storage-1": {generation: 1},
						"storage-2": {generation: 0},
					},
				},
				{
					relativePath: "repository-2",
					replicas: replicas{
						"storage-1": {generation: 1},
						"storage-2": {generation: 0},
					},
				},
			},
			count: 2,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			tx, err := getDB(t).Begin()
			require.NoError(t, err)
			defer tx.Rollback()

			testhelper.SetHealthyNodes(t, ctx, tx, map[string]map[string][]string{
				"praefect-0": {"virtual-storage-1": tc.healthyNodes},
			})

			for _, repository := range tc.repositories {
				if !repository.deleted {
					_, err := tx.ExecContext(ctx, `
						INSERT INTO repositories (virtual_storage, relative_path)
						VALUES ('virtual-storage-1', $1)
					`, repository.relativePath,
					)
					require.NoError(t, err)
				}

				for storage, replica := range repository.replicas {
					if replica.assigned {
						_, err := tx.ExecContext(ctx, `
							INSERT INTO repository_assignments (virtual_storage, relative_path, storage)
							VALUES ('virtual-storage-1', $1, $2)
						`, repository.relativePath, storage,
						)
						require.NoError(t, err)
					}

					_, err := tx.ExecContext(ctx, `
						INSERT INTO storage_repositories (virtual_storage, relative_path, storage, generation)
						VALUES ('virtual-storage-1', $1, $2, $3)
					`, repository.relativePath, storage, replica.generation,
					)
					require.NoError(t, err)
				}
			}

			c := NewRepositoryStoreCollector(logrus.New(), []string{"virtual-storage-1", "virtual-storage-2"}, tx)
			require.NoError(t, testutil.CollectAndCompare(c, strings.NewReader(fmt.Sprintf(`
# HELP gitaly_praefect_read_only_repositories Number of repositories in read-only mode within a virtual storage.
# TYPE gitaly_praefect_read_only_repositories gauge
gitaly_praefect_read_only_repositories{virtual_storage="virtual-storage-1"} %d
gitaly_praefect_read_only_repositories{virtual_storage="virtual-storage-2"} 0
# HELP gitaly_praefect_unavailable_repositories Number of repositories that have no healthy, up to date replicas.
# TYPE gitaly_praefect_unavailable_repositories gauge
gitaly_praefect_unavailable_repositories{virtual_storage="virtual-storage-1"} %d
gitaly_praefect_unavailable_repositories{virtual_storage="virtual-storage-2"} 0
			`, tc.count, tc.count))))
		})
	}
}
