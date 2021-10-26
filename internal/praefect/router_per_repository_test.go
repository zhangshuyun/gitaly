package praefect

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/commonerr"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// StaticRepositoryAssignments is a static assignment of storages for each individual repository.
type StaticRepositoryAssignments map[string]map[string][]string

func (st StaticRepositoryAssignments) GetHostAssignments(ctx context.Context, virtualStorage, relativePath string) ([]string, error) {
	vs, ok := st[virtualStorage]
	if !ok {
		return nil, nodes.ErrVirtualStorageNotExist
	}

	storages, ok := vs[relativePath]
	if !ok {
		return nil, errRepositoryNotFound
	}

	return storages, nil
}

// PrimaryGetter is an adapter to turn conforming functions in to a PrimaryGetter.
type PrimaryGetterFunc func(ctx context.Context, virtualStorage, relativePath string) (string, error)

func (fn PrimaryGetterFunc) GetPrimary(ctx context.Context, virtualStorage, relativePath string) (string, error) {
	return fn(ctx, virtualStorage, relativePath)
}

func TestPerRepositoryRouter_RouteStorageAccessor(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	for _, tc := range []struct {
		desc           string
		virtualStorage string
		numCandidates  int
		pickCandidate  int
		error          error
		node           string
	}{
		{
			desc:           "unknown virtual storage",
			virtualStorage: "unknown",
			error:          nodes.ErrVirtualStorageNotExist,
		},
		{
			desc:           "picks randomly first candidate",
			virtualStorage: "virtual-storage-1",
			numCandidates:  2,
			pickCandidate:  0,
			node:           "valid-choice-1",
		},
		{
			desc:           "picks randomly second candidate",
			virtualStorage: "virtual-storage-1",
			numCandidates:  2,
			pickCandidate:  1,
			node:           "valid-choice-2",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			conns := Connections{
				"virtual-storage-1": {
					"valid-choice-1": &grpc.ClientConn{},
					"valid-choice-2": &grpc.ClientConn{},
					"unhealthy":      &grpc.ClientConn{},
				},
			}

			router := NewPerRepositoryRouter(
				conns,
				nil,
				StaticHealthChecker{
					"virtual-storage-1": {
						"valid-choice-1",
						"valid-choice-2",
					},
				},
				mockRandom{
					intnFunc: func(n int) int {
						require.Equal(t, tc.numCandidates, n)
						return tc.pickCandidate
					},
				},
				nil,
				nil,
				datastore.MockRepositoryStore{},
				nil,
			)

			node, err := router.RouteStorageAccessor(ctx, tc.virtualStorage)
			require.Equal(t, tc.error, err)
			require.Equal(t, RouterNode{
				Storage:    tc.node,
				Connection: conns["virtual-storage-1"][tc.node],
			}, node)
		})
	}
}

func TestPerRepositoryRouter_RouteRepositoryAccessor(t *testing.T) {
	t.Parallel()

	db := glsql.NewDB(t)

	const relativePath = "repository"

	for _, tc := range []struct {
		desc           string
		virtualStorage string
		healthyNodes   StaticHealthChecker
		metadata       map[string]string
		forcePrimary   bool
		numCandidates  int
		pickCandidate  int
		error          error
		node           string
	}{
		{
			desc:           "unknown virtual storage",
			virtualStorage: "unknown",
			error:          nodes.ErrVirtualStorageNotExist,
		},
		{
			desc:           "no healthy nodes",
			virtualStorage: "virtual-storage-1",
			healthyNodes:   map[string][]string{},
			error:          ErrNoHealthyNodes,
		},
		{
			desc:           "primary picked randomly",
			virtualStorage: "virtual-storage-1",
			healthyNodes: map[string][]string{
				"virtual-storage-1": {"primary", "consistent-secondary"},
			},
			numCandidates: 2,
			pickCandidate: 0,
			node:          "primary",
		},
		{
			desc:           "secondary picked randomly",
			virtualStorage: "virtual-storage-1",
			healthyNodes: map[string][]string{
				"virtual-storage-1": {"primary", "consistent-secondary"},
			},
			numCandidates: 2,
			pickCandidate: 1,
			node:          "consistent-secondary",
		},
		{
			desc:           "secondary picked when primary is unhealthy",
			virtualStorage: "virtual-storage-1",
			healthyNodes: map[string][]string{
				"virtual-storage-1": {"consistent-secondary"},
			},
			numCandidates: 1,
			node:          "consistent-secondary",
		},
		{
			desc:           "no suitable nodes",
			virtualStorage: "virtual-storage-1",
			healthyNodes: map[string][]string{
				"virtual-storage-1": {"inconsistent-secondary"},
			},
			error: ErrNoSuitableNode,
		},
		{
			desc:           "primary force-picked",
			virtualStorage: "virtual-storage-1",
			healthyNodes: map[string][]string{
				"virtual-storage-1": {"primary", "consistent-secondary"},
			},
			forcePrimary: true,
			node:         "primary",
		},
		{
			desc:           "secondary not picked if force-picking unhealthy primary",
			virtualStorage: "virtual-storage-1",
			healthyNodes: map[string][]string{
				"virtual-storage-1": {"consistent-secondary"},
			},
			forcePrimary: true,
			error:        nodes.ErrPrimaryNotHealthy,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			ctx = testhelper.MergeIncomingMetadata(ctx, metadata.New(tc.metadata))

			conns := Connections{
				"virtual-storage-1": {
					"primary":                &grpc.ClientConn{},
					"consistent-secondary":   &grpc.ClientConn{},
					"inconsistent-secondary": &grpc.ClientConn{},
					"unhealthy-secondary":    &grpc.ClientConn{},
				},
			}

			tx := db.Begin(t)
			defer tx.Rollback(t)

			testdb.SetHealthyNodes(t, ctx, tx, map[string]map[string][]string{"praefect": {
				"virtual-storage-1": {"primary", "consistent-secondary", "inconsistent-secondary"},
			}})

			rs := datastore.NewPostgresRepositoryStore(tx, nil)
			repositoryID, err := rs.ReserveRepositoryID(ctx, "virtual-storage-1", relativePath)
			require.NoError(t, err)
			require.NoError(t,
				rs.CreateRepository(ctx, repositoryID, "virtual-storage-1", relativePath, "primary",
					[]string{"consistent-secondary", "unhealthy-secondary", "inconsistent-secondary"}, nil, true, true),
			)
			require.NoError(t,
				rs.IncrementGeneration(ctx, repositoryID, "primary", []string{"consistent-secondary", "unhealthy-secondary"}),
			)

			router := NewPerRepositoryRouter(
				conns,
				nodes.NewPerRepositoryElector(tx),
				tc.healthyNodes,
				mockRandom{
					intnFunc: func(n int) int {
						require.Equal(t, tc.numCandidates, n)
						return tc.pickCandidate
					},
				},
				rs,
				nil,
				rs,
				nil,
			)

			route, err := router.RouteRepositoryAccessor(ctx, tc.virtualStorage, relativePath, tc.forcePrimary)
			require.Equal(t, tc.error, err)
			if tc.node != "" {
				require.Equal(t,
					RepositoryAccessorRoute{
						ReplicaPath: relativePath,
						Node: RouterNode{
							Storage:    tc.node,
							Connection: conns[tc.virtualStorage][tc.node],
						},
					},
					route)
			} else {
				require.Empty(t, route)
			}
		})
	}
}

func TestPerRepositoryRouter_RouteRepositoryMutator(t *testing.T) {
	t.Parallel()

	db := glsql.NewDB(t)

	configuredNodes := map[string][]string{
		"virtual-storage-1": {"primary", "secondary-1", "secondary-2"},
	}

	for _, tc := range []struct {
		desc               string
		virtualStorage     string
		healthyNodes       StaticHealthChecker
		consistentStorages []string
		secondaries        []string
		replicationTargets []string
		error              error
		assignedNodes      StaticRepositoryAssignments
	}{
		{
			desc:           "unknown virtual storage",
			virtualStorage: "unknown",
			error:          nodes.ErrVirtualStorageNotExist,
		},
		{
			desc:               "outdated primary is demoted",
			virtualStorage:     "virtual-storage-1",
			healthyNodes:       StaticHealthChecker{"virtual-storage-1": {"primary", "secondary-2"}},
			consistentStorages: []string{"secondary-1"},
			error:              nodes.ErrPrimaryNotHealthy,
		},
		{
			desc:               "primary unhealthy",
			virtualStorage:     "virtual-storage-1",
			healthyNodes:       StaticHealthChecker{"virtual-storage-1": {"secondary-1", "secondary-2"}},
			consistentStorages: []string{"primary"},
			error:              nodes.ErrPrimaryNotHealthy,
		},
		{
			desc:               "all secondaries consistent",
			virtualStorage:     "virtual-storage-1",
			healthyNodes:       StaticHealthChecker(configuredNodes),
			consistentStorages: []string{"primary", "secondary-1", "secondary-2"},
			secondaries:        []string{"secondary-1", "secondary-2"},
		},
		{
			desc:               "inconsistent secondary",
			virtualStorage:     "virtual-storage-1",
			healthyNodes:       StaticHealthChecker(configuredNodes),
			consistentStorages: []string{"primary", "secondary-2"},
			secondaries:        []string{"secondary-2"},
			replicationTargets: []string{"secondary-1"},
		},
		{
			desc:               "unhealthy secondaries",
			virtualStorage:     "virtual-storage-1",
			healthyNodes:       StaticHealthChecker{"virtual-storage-1": {"primary"}},
			consistentStorages: []string{"primary", "secondary-1"},
			replicationTargets: []string{"secondary-1", "secondary-2"},
		},
		{
			desc:               "up to date unassigned nodes are ignored",
			virtualStorage:     "virtual-storage-1",
			healthyNodes:       StaticHealthChecker(configuredNodes),
			assignedNodes:      StaticRepositoryAssignments{"virtual-storage-1": {"repository": {"primary", "secondary-1"}}},
			consistentStorages: []string{"primary", "secondary-1", "secondary-2"},
			secondaries:        []string{"secondary-1"},
		},
		{
			desc:               "outdated unassigned nodes are ignored",
			virtualStorage:     "virtual-storage-1",
			healthyNodes:       StaticHealthChecker(configuredNodes),
			assignedNodes:      StaticRepositoryAssignments{"virtual-storage-1": {"repository": {"primary", "secondary-1"}}},
			consistentStorages: []string{"primary", "secondary-1"},
			secondaries:        []string{"secondary-1"},
		},
		{
			desc:               "primary is unassigned",
			virtualStorage:     "virtual-storage-1",
			healthyNodes:       StaticHealthChecker(configuredNodes),
			assignedNodes:      StaticRepositoryAssignments{"virtual-storage-1": {"repository": {"secondary-1", "secondary-2"}}},
			consistentStorages: []string{"primary"},
			error:              errPrimaryUnassigned,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			conns := Connections{
				"virtual-storage-1": {
					"primary":     &grpc.ClientConn{},
					"secondary-1": &grpc.ClientConn{},
					"secondary-2": &grpc.ClientConn{},
				},
			}

			tx := db.Begin(t)
			defer tx.Rollback(t)

			testdb.SetHealthyNodes(t, ctx, tx, map[string]map[string][]string{"praefect": configuredNodes})

			const relativePath = "repository"

			rs := datastore.NewPostgresRepositoryStore(tx, nil)
			repositoryID, err := rs.ReserveRepositoryID(ctx, "virtual-storage-1", relativePath)
			require.NoError(t, err)

			require.NoError(t,
				rs.CreateRepository(ctx, repositoryID, "virtual-storage-1", relativePath, "primary", []string{"secondary-1", "secondary-2"}, nil, true, false),
			)

			if len(tc.consistentStorages) > 0 {
				require.NoError(t, rs.IncrementGeneration(ctx, repositoryID, tc.consistentStorages[0], tc.consistentStorages[1:]))
			}

			for virtualStorage, relativePaths := range tc.assignedNodes {
				for relativePath, storages := range relativePaths {
					for _, storage := range storages {
						_, err := tx.ExecContext(ctx, `
							INSERT INTO repository_assignments
							VALUES ($1, $2, $3, $4)
						`, virtualStorage, relativePath, storage, repositoryID)
						require.NoError(t, err)
					}
				}
			}

			router := NewPerRepositoryRouter(
				conns,
				nodes.NewPerRepositoryElector(tx),
				tc.healthyNodes,
				nil,
				rs,
				datastore.NewAssignmentStore(tx, configuredNodes),
				rs,
				nil,
			)

			route, err := router.RouteRepositoryMutator(ctx, tc.virtualStorage, relativePath)
			require.Equal(t, tc.error, err)
			if err == nil {
				var secondaries []RouterNode
				for _, secondary := range tc.secondaries {
					secondaries = append(secondaries, RouterNode{
						Storage:    secondary,
						Connection: conns[tc.virtualStorage][secondary],
					})
				}

				require.Equal(t, RepositoryMutatorRoute{
					RepositoryID: repositoryID,
					ReplicaPath:  relativePath,
					Primary: RouterNode{
						Storage:    "primary",
						Connection: conns[tc.virtualStorage]["primary"],
					},
					Secondaries:        secondaries,
					ReplicationTargets: tc.replicationTargets,
				}, route)
			}
		})
	}
}

func TestPerRepositoryRouter_RouteRepositoryCreation(t *testing.T) {
	t.Parallel()

	configuredNodes := map[string][]string{
		"virtual-storage-1": {"primary", "secondary-1", "secondary-2"},
	}

	type matcher func(*testing.T, RepositoryMutatorRoute)

	requireOneOf := func(expected ...RepositoryMutatorRoute) matcher {
		return func(t *testing.T, actual RepositoryMutatorRoute) {
			sort.Slice(actual.Secondaries, func(i, j int) bool {
				return actual.Secondaries[i].Storage < actual.Secondaries[j].Storage
			})
			sort.Strings(actual.ReplicationTargets)
			require.Contains(t, expected, actual)
		}
	}

	primaryConn := &grpc.ClientConn{}
	secondary1Conn := &grpc.ClientConn{}
	secondary2Conn := &grpc.ClientConn{}

	db := glsql.NewDB(t)

	const relativePath = "relative-path"

	for _, tc := range []struct {
		desc                string
		virtualStorage      string
		healthyNodes        StaticHealthChecker
		replicationFactor   int
		primaryCandidates   int
		primaryPick         int
		secondaryCandidates int
		repositoryExists    bool
		matchRoute          matcher
		error               error
	}{
		{
			desc:           "no healthy nodes",
			virtualStorage: "virtual-storage-1",
			healthyNodes:   StaticHealthChecker{},
			error:          ErrNoHealthyNodes,
		},
		{
			desc:           "invalid virtual storage",
			virtualStorage: "invalid",
			error:          nodes.ErrVirtualStorageNotExist,
		},
		{
			desc:              "no healthy secondaries",
			virtualStorage:    "virtual-storage-1",
			healthyNodes:      StaticHealthChecker{"virtual-storage-1": {"primary"}},
			primaryCandidates: 1,
			primaryPick:       0,
			matchRoute: requireOneOf(
				RepositoryMutatorRoute{
					RepositoryID:       1,
					ReplicaPath:        relativePath,
					Primary:            RouterNode{Storage: "primary", Connection: primaryConn},
					ReplicationTargets: []string{"secondary-1", "secondary-2"},
				},
			),
		},
		{
			desc:              "success with all secondaries healthy",
			virtualStorage:    "virtual-storage-1",
			healthyNodes:      StaticHealthChecker(configuredNodes),
			primaryCandidates: 3,
			primaryPick:       0,
			matchRoute: requireOneOf(
				RepositoryMutatorRoute{
					RepositoryID: 1,
					ReplicaPath:  relativePath,
					Primary:      RouterNode{Storage: "primary", Connection: primaryConn},
					Secondaries: []RouterNode{
						{Storage: "secondary-1", Connection: secondary1Conn},
						{Storage: "secondary-2", Connection: secondary2Conn},
					},
				},
			),
		},
		{
			desc:              "success with one secondary unhealthy",
			virtualStorage:    "virtual-storage-1",
			healthyNodes:      StaticHealthChecker{"virtual-storage-1": {"primary", "secondary-1"}},
			primaryCandidates: 2,
			primaryPick:       0,
			matchRoute: requireOneOf(
				RepositoryMutatorRoute{
					RepositoryID: 1,
					ReplicaPath:  relativePath,
					Primary:      RouterNode{Storage: "primary", Connection: primaryConn},
					Secondaries: []RouterNode{
						{Storage: "secondary-1", Connection: secondary1Conn},
					},
					ReplicationTargets: []string{"secondary-2"},
				},
			),
		},
		{
			desc:              "replication factor of one configured",
			virtualStorage:    "virtual-storage-1",
			healthyNodes:      StaticHealthChecker(configuredNodes),
			replicationFactor: 1,
			primaryCandidates: 3,
			primaryPick:       0,
			matchRoute: requireOneOf(
				RepositoryMutatorRoute{
					RepositoryID: 1,
					ReplicaPath:  relativePath,
					Primary:      RouterNode{Storage: "primary", Connection: primaryConn},
				},
			),
		},
		{
			desc:                "replication factor of two configured",
			virtualStorage:      "virtual-storage-1",
			healthyNodes:        StaticHealthChecker(configuredNodes),
			replicationFactor:   2,
			primaryCandidates:   3,
			primaryPick:         0,
			secondaryCandidates: 2,
			matchRoute: requireOneOf(
				RepositoryMutatorRoute{
					RepositoryID: 1,
					ReplicaPath:  relativePath,
					Primary:      RouterNode{Storage: "primary", Connection: primaryConn},
					Secondaries:  []RouterNode{{Storage: "secondary-1", Connection: secondary1Conn}},
				},
				RepositoryMutatorRoute{
					RepositoryID: 1,
					ReplicaPath:  relativePath,
					Primary:      RouterNode{Storage: "primary", Connection: primaryConn},
					Secondaries:  []RouterNode{{Storage: "secondary-2", Connection: secondary1Conn}},
				},
			),
		},
		{
			desc:                "replication factor of three configured with unhealthy secondary",
			virtualStorage:      "virtual-storage-1",
			healthyNodes:        StaticHealthChecker{"virtual-storage-1": {"primary", "secondary-1"}},
			replicationFactor:   3,
			primaryCandidates:   2,
			primaryPick:         0,
			secondaryCandidates: 2,
			matchRoute: requireOneOf(
				RepositoryMutatorRoute{
					RepositoryID:       1,
					ReplicaPath:        relativePath,
					Primary:            RouterNode{Storage: "primary", Connection: primaryConn},
					Secondaries:        []RouterNode{{Storage: "secondary-1", Connection: secondary1Conn}},
					ReplicationTargets: []string{"secondary-2"},
				},
			),
		},
		{
			desc:              "repository already exists",
			virtualStorage:    "virtual-storage-1",
			healthyNodes:      StaticHealthChecker(configuredNodes),
			primaryCandidates: 3,
			primaryPick:       0,
			repositoryExists:  true,
			error:             fmt.Errorf("reserve repository id: %w", commonerr.ErrRepositoryAlreadyExists),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			db.TruncateAll(t)

			rs := datastore.NewPostgresRepositoryStore(db, nil)
			if tc.repositoryExists {
				require.NoError(t,
					rs.CreateRepository(ctx, 1, "virtual-storage-1", relativePath, "primary", nil, nil, true, true),
				)
			}

			route, err := NewPerRepositoryRouter(
				Connections{
					"virtual-storage-1": {
						"primary":     primaryConn,
						"secondary-1": secondary1Conn,
						"secondary-2": secondary2Conn,
					},
				},
				nil,
				tc.healthyNodes,
				mockRandom{
					intnFunc: func(n int) int {
						require.Equal(t, tc.primaryCandidates, n)
						return tc.primaryPick
					},
					shuffleFunc: func(n int, swap func(i, j int)) {
						require.Equal(t, tc.secondaryCandidates, n)
					},
				},
				nil,
				nil,
				rs,
				map[string]int{"virtual-storage-1": tc.replicationFactor},
			).RouteRepositoryCreation(ctx, tc.virtualStorage, relativePath)
			if tc.error != nil {
				require.Equal(t, tc.error, err)
				return
			}

			require.NoError(t, err)
			tc.matchRoute(t, route)
		})
	}
}
