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

	db := getDB(t)
	rs := NewPostgresRepositoryStore(db, nil)

	state := map[string]map[string]map[string]int{
		"some-read-only": {
			"read-only": {
				"vs-primary":   0,
				"repo-primary": 0,
				"secondary":    1,
			},
			"writable": {
				"vs-primary":   1,
				"repo-primary": 1,
				"secondary":    1,
			},
			"repo-writable": {
				"vs-primary":   0,
				"repo-primary": 1,
				"secondary":    1,
			},
		},
		"all-writable": {
			"writable": {
				"vs-primary":   0,
				"repo-primary": 0,
			},
		},
		"unconfigured": {
			"read-only": {
				"secondary": 1,
			},
		},
		"no-records": {},
		"no-primary": {
			"read-only": {
				"secondary": 0,
			},
		},
	}
	for virtualStorage, relativePaths := range state {
		demoted := false
		if virtualStorage == "no-primary" {
			demoted = true
		}
		_, err := db.ExecContext(ctx, `
			INSERT INTO shard_primaries (shard_name, node_name, elected_by_praefect, elected_at, demoted)
			VALUES ($1, 'vs-primary', 'not-needed', now(), $2)
			`, virtualStorage, demoted,
		)
		require.NoError(t, err)

		for relativePath, storages := range relativePaths {
			if virtualStorage != "no-primary" {
				_, err := db.ExecContext(ctx, `
						INSERT INTO repositories (virtual_storage, relative_path, "primary")
						VALUES ($1, $2, 'repo-primary')
						`, virtualStorage, relativePath,
				)
				require.NoError(t, err)
			}

			for storage, generation := range storages {
				require.NoError(t, rs.SetGeneration(ctx, virtualStorage, relativePath, storage, generation))
			}
		}
	}

	var virtualStorages []string
	for vs := range state {
		if vs == "unconfigured" {
			continue
		}

		virtualStorages = append(virtualStorages, vs)
	}

	for _, tc := range []struct {
		desc              string
		repositoryScoped  bool
		someReadOnlyCount int
	}{
		{
			desc:              "repository scoped",
			someReadOnlyCount: 1,
			repositoryScoped:  true,
		},
		{
			desc:              "virtual storage scoped",
			someReadOnlyCount: 2,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			c := NewRepositoryStoreCollector(logrus.New(), virtualStorages, db, tc.repositoryScoped)
			require.NoError(t, testutil.CollectAndCompare(c, strings.NewReader(fmt.Sprintf(`
# HELP gitaly_praefect_read_only_repositories Number of repositories in read-only mode within a virtual storage.
# TYPE gitaly_praefect_read_only_repositories gauge
gitaly_praefect_read_only_repositories{virtual_storage="all-writable"} 0
gitaly_praefect_read_only_repositories{virtual_storage="no-records"} 0
gitaly_praefect_read_only_repositories{virtual_storage="no-primary"} 1
gitaly_praefect_read_only_repositories{virtual_storage="some-read-only"} %d
`, tc.someReadOnlyCount))))
		})
	}
}
