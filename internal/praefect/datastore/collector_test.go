// +build postgres

package datastore

import (
	"fmt"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

// HealthCheckerFunc is an adapter to turn a conforming function in to a HealthChecker.
type StaticHealthChecker map[string][]string

func (hc StaticHealthChecker) HealthyNodes() map[string][]string { return hc }

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
			c := NewRepositoryStoreCollector(logrus.New(), virtualStorages, db, tc.repositoryScoped, nil)
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

func TestRepositoryStoreCollector_lazy_failover(t *testing.T) {
	for _, tc := range []struct {
		desc                      string
		existingGenerations       map[string]int
		existingAssignments       []string
		noPrimary                 bool
		healthyNodes              StaticHealthChecker
		repositoryScopedCount     int
		virtualStorageScopedCount int
	}{
		{
			desc: "no records",
		},
		{
			desc: "outdated secondary",
			existingGenerations: map[string]int{
				"primary": 0,
			},
		},
		{
			desc:      "no primary with a candidate",
			noPrimary: true,
			existingGenerations: map[string]int{
				"secondary": 1,
			},
			repositoryScopedCount:     0,
			virtualStorageScopedCount: 1,
		},
		{
			desc:         "no primary with no candidate",
			noPrimary:    true,
			healthyNodes: StaticHealthChecker{"virtual-storage": {"primary"}},
			existingGenerations: map[string]int{
				"secondary": 1,
			},
			repositoryScopedCount:     1,
			virtualStorageScopedCount: 1,
		},
		{
			desc:         "no record unhealthy primary",
			healthyNodes: StaticHealthChecker{"virtual-storage": {"secondary"}},
			existingGenerations: map[string]int{
				"secondary": 0,
			},
			repositoryScopedCount:     0,
			virtualStorageScopedCount: 1,
		},
		{
			desc: "no record healthy primary",
			existingGenerations: map[string]int{
				"secondary": 0,
			},
			repositoryScopedCount:     1,
			virtualStorageScopedCount: 1,
		},
		{
			desc:         "outdated unhealthy primary",
			healthyNodes: StaticHealthChecker{"virtual-storage": {"secondary"}},
			existingGenerations: map[string]int{
				"primary":   0,
				"secondary": 1,
			},
			repositoryScopedCount:     0,
			virtualStorageScopedCount: 1,
		},
		{
			desc: "outdated healthy primary",
			existingGenerations: map[string]int{
				"primary":   0,
				"secondary": 1,
			},
			repositoryScopedCount:     1,
			virtualStorageScopedCount: 1,
		},
		{
			desc:         "in-sync unhealthy primary with a candidate",
			healthyNodes: StaticHealthChecker{"virtual-storage": {"secondary"}},
			existingGenerations: map[string]int{
				"primary":   0,
				"secondary": 0,
			},
			repositoryScopedCount:     0,
			virtualStorageScopedCount: 0,
		},
		{
			desc:         "in-sync unhealthy primary without a candidate",
			healthyNodes: StaticHealthChecker{"virtual-storage": {}},
			existingGenerations: map[string]int{
				"primary":   0,
				"secondary": 0,
			},
			repositoryScopedCount:     1,
			virtualStorageScopedCount: 0,
		},
		{
			desc:         "secondary is unassigned",
			healthyNodes: StaticHealthChecker{"virtual-storage": {"secondary"}},
			existingGenerations: map[string]int{
				"primary":   1,
				"secondary": 1,
			},
			existingAssignments:       []string{"primary"},
			repositoryScopedCount:     1,
			virtualStorageScopedCount: 0,
		},
		{
			desc:         "failsover to assigned",
			healthyNodes: StaticHealthChecker{"virtual-storage": {"secondary"}},
			existingGenerations: map[string]int{
				"primary":   1,
				"secondary": 1,
			},
			existingAssignments:       []string{"primary", "secondary"},
			repositoryScopedCount:     0,
			virtualStorageScopedCount: 0,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			db := getDB(t)

			staticQueries := []string{
				`
					INSERT INTO repositories (virtual_storage, relative_path)
					VALUES ('virtual-storage', 'repository')
				`,
			}

			if !tc.noPrimary {
				staticQueries = append(staticQueries,
					`
						UPDATE repositories SET "primary" = 'primary'
					`,
					`
						INSERT INTO shard_primaries (shard_name, node_name, elected_by_praefect, elected_at)
						VALUES ('virtual-storage', 'primary', 'ignored', now())
					`,
				)
			}

			for _, q := range staticQueries {
				_, err := db.ExecContext(ctx, q)
				require.NoError(t, err)
			}

			for storage, generation := range tc.existingGenerations {
				_, err := db.ExecContext(ctx, `
					INSERT INTO storage_repositories (virtual_storage, relative_path, storage, generation)
					VALUES ('virtual-storage', 'repository', $1, $2)
				`, storage, generation)
				require.NoError(t, err)
			}

			for _, storage := range tc.existingAssignments {
				_, err := db.ExecContext(ctx, `
					INSERT INTO repository_assignments (virtual_storage, relative_path, storage)
					VALUES ('virtual-storage', 'repository', $1)
				`, storage)
				require.NoError(t, err)
			}

			for _, scope := range []struct {
				desc             string
				repositoryScoped bool
				readOnlyCount    int
			}{
				{
					desc:             "virtual storage primaries",
					repositoryScoped: false,
					readOnlyCount:    tc.virtualStorageScopedCount,
				},
				{
					desc:             "repository primaries",
					repositoryScoped: true,
					readOnlyCount:    tc.repositoryScopedCount,
				},
			} {
				t.Run(scope.desc, func(t *testing.T) {
					healthyNodes := StaticHealthChecker{"virtual-storage": {"primary", "secondary"}}
					if tc.healthyNodes != nil {
						healthyNodes = tc.healthyNodes
					}

					c := NewRepositoryStoreCollector(
						logrus.New(),
						[]string{"virtual-storage"},
						db,
						scope.repositoryScoped,
						healthyNodes,
					)

					require.NoError(t, testutil.CollectAndCompare(c, strings.NewReader(fmt.Sprintf(`
# HELP gitaly_praefect_read_only_repositories Number of repositories in read-only mode within a virtual storage.
# TYPE gitaly_praefect_read_only_repositories gauge
gitaly_praefect_read_only_repositories{virtual_storage="virtual-storage"} %d
					`, scope.readOnlyCount))))
				})
			}
		})
	}
}
