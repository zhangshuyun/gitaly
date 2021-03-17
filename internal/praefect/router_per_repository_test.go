package praefect

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
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
				"virtual-storage-1": {"inconistent-secondary"},
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
					"primary":               &grpc.ClientConn{},
					"consistent-secondary":  &grpc.ClientConn{},
					"inconistent-secondary": &grpc.ClientConn{},
					"unhealthy-secondary":   &grpc.ClientConn{},
				},
			}

			router := NewPerRepositoryRouter(
				conns,
				PrimaryGetterFunc(func(ctx context.Context, virtualStorage, relativePath string) (string, error) {
					t.Helper()
					require.Equal(t, tc.virtualStorage, virtualStorage)
					require.Equal(t, "repository", relativePath)
					return "primary", nil
				}),
				tc.healthyNodes,
				mockRandom{
					intnFunc: func(n int) int {
						require.Equal(t, tc.numCandidates, n)
						return tc.pickCandidate
					},
				},
				datastore.MockRepositoryStore{
					GetConsistentStoragesFunc: func(ctx context.Context, virtualStorage, relativePath string) (map[string]struct{}, error) {
						t.Helper()
						require.Equal(t, tc.virtualStorage, virtualStorage)
						require.Equal(t, "repository", relativePath)
						return map[string]struct{}{"primary": {}, "consistent-secondary": {}}, nil
					},
				},
				nil,
				nil,
			)

			node, err := router.RouteRepositoryAccessor(ctx, tc.virtualStorage, "repository", tc.forcePrimary)
			require.Equal(t, tc.error, err)
			if tc.node != "" {
				require.Equal(t, RouterNode{
					Storage:    tc.node,
					Connection: conns[tc.virtualStorage][tc.node],
				}, node)
			} else {
				require.Empty(t, node)
			}
		})
	}
}

func TestPerRepositoryRouter_RouteRepositoryMutator(t *testing.T) {
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
		assignedNodes      AssignmentGetter
	}{
		{
			desc:           "unknown virtual storage",
			virtualStorage: "unknown",
			error:          nodes.ErrVirtualStorageNotExist,
		},
		{
			desc:               "primary outdated",
			virtualStorage:     "virtual-storage-1",
			healthyNodes:       StaticHealthChecker(configuredNodes),
			assignedNodes:      StaticStorageAssignments(configuredNodes),
			consistentStorages: []string{"secondary-1", "secondary-2"},
			error:              ErrRepositoryReadOnly,
		},
		{
			desc:               "primary unhealthy",
			virtualStorage:     "virtual-storage-1",
			healthyNodes:       StaticHealthChecker{"virtual-storage-1": {"secondary-1", "secondary-2"}},
			assignedNodes:      StaticStorageAssignments(configuredNodes),
			consistentStorages: []string{"primary", "secondary-1", "secondary-2"},
			error:              nodes.ErrPrimaryNotHealthy,
		},
		{
			desc:               "all secondaries consistent",
			virtualStorage:     "virtual-storage-1",
			healthyNodes:       StaticHealthChecker(configuredNodes),
			assignedNodes:      StaticStorageAssignments(configuredNodes),
			consistentStorages: []string{"primary", "secondary-1", "secondary-2"},
			secondaries:        []string{"secondary-1", "secondary-2"},
		},
		{
			desc:               "inconsistent secondary",
			virtualStorage:     "virtual-storage-1",
			healthyNodes:       StaticHealthChecker(configuredNodes),
			assignedNodes:      StaticStorageAssignments(configuredNodes),
			consistentStorages: []string{"primary", "secondary-2"},
			secondaries:        []string{"secondary-2"},
			replicationTargets: []string{"secondary-1"},
		},
		{
			desc:               "unhealthy secondaries",
			virtualStorage:     "virtual-storage-1",
			healthyNodes:       StaticHealthChecker{"virtual-storage-1": {"primary"}},
			assignedNodes:      StaticStorageAssignments(configuredNodes),
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
			consistentStorages: []string{"primary", "secondary-1", "secondary-2"},
			secondaries:        []string{"secondary-1"},
			replicationTargets: []string{"secondary-2"},
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

			router := NewPerRepositoryRouter(
				conns,
				PrimaryGetterFunc(func(ctx context.Context, virtualStorage, relativePath string) (string, error) {
					t.Helper()
					require.Equal(t, tc.virtualStorage, virtualStorage)
					require.Equal(t, "repository", relativePath)
					return "primary", nil
				}),
				tc.healthyNodes,
				nil,
				datastore.MockRepositoryStore{
					GetConsistentStoragesFunc: func(ctx context.Context, virtualStorage, relativePath string) (map[string]struct{}, error) {
						t.Helper()
						require.Equal(t, tc.virtualStorage, virtualStorage)
						require.Equal(t, "repository", relativePath)
						consistentStorages := map[string]struct{}{}
						for _, storage := range tc.consistentStorages {
							consistentStorages[storage] = struct{}{}
						}

						return consistentStorages, nil
					},
				},
				tc.assignedNodes,
				nil,
			)

			route, err := router.RouteRepositoryMutator(ctx, tc.virtualStorage, "repository")
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
	configuredNodes := map[string][]string{
		"virtual-storage-1": {"primary", "secondary-1", "secondary-2"},
	}

	type matcher func(*testing.T, []string)

	requireOneOf := func(expected ...[]string) func(*testing.T, []string) {
		return func(t *testing.T, actual []string) {
			sort.Strings(actual)
			require.Contains(t, expected, actual)
		}
	}

	requireNil := func(t *testing.T, actual []string) {
		require.Nil(t, actual)
	}

	for _, tc := range []struct {
		desc                      string
		virtualStorage            string
		healthyNodes              StaticHealthChecker
		replicationFactor         int
		primaryCandidates         int
		primaryPick               int
		secondaryCandidates       int
		primary                   string
		requireReplicationTargets matcher
		error                     error
	}{
		{
			desc:                      "no healthy nodes",
			virtualStorage:            "virtual-storage-1",
			healthyNodes:              StaticHealthChecker{},
			error:                     ErrNoHealthyNodes,
			requireReplicationTargets: requireNil,
		},
		{
			desc:                      "invalid virtual storage",
			virtualStorage:            "invalid",
			error:                     nodes.ErrVirtualStorageNotExist,
			requireReplicationTargets: requireNil,
		},
		{
			desc:                      "no healthy secondaries",
			virtualStorage:            "virtual-storage-1",
			healthyNodes:              StaticHealthChecker{"virtual-storage-1": {"primary"}},
			primaryCandidates:         1,
			primaryPick:               0,
			primary:                   "primary",
			requireReplicationTargets: requireOneOf([]string{"secondary-1", "secondary-2"}),
		},
		{
			desc:                      "success",
			virtualStorage:            "virtual-storage-1",
			healthyNodes:              StaticHealthChecker(configuredNodes),
			primaryCandidates:         3,
			primaryPick:               0,
			primary:                   "primary",
			requireReplicationTargets: requireOneOf([]string{"secondary-1", "secondary-2"}),
		},
		{
			desc:                      "replication factor of one configured",
			virtualStorage:            "virtual-storage-1",
			healthyNodes:              StaticHealthChecker(configuredNodes),
			replicationFactor:         1,
			primaryCandidates:         3,
			primaryPick:               0,
			primary:                   "primary",
			requireReplicationTargets: requireNil,
		},
		{
			desc:                      "replication factor of two configured",
			virtualStorage:            "virtual-storage-1",
			healthyNodes:              StaticHealthChecker(configuredNodes),
			replicationFactor:         2,
			primaryCandidates:         3,
			primaryPick:               0,
			secondaryCandidates:       2,
			primary:                   "primary",
			requireReplicationTargets: requireOneOf([]string{"secondary-1"}, []string{"secondary-2"}),
		},
		{
			desc:                      "replication factor of three configured with unhealthy secondary",
			virtualStorage:            "virtual-storage-1",
			healthyNodes:              StaticHealthChecker{"virtual-storage-1": {"primary", "secondary-1"}},
			replicationFactor:         3,
			primaryCandidates:         2,
			primaryPick:               0,
			secondaryCandidates:       2,
			primary:                   "primary",
			requireReplicationTargets: requireOneOf([]string{"secondary-1", "secondary-2"}),
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

			route, err := NewPerRepositoryRouter(
				conns,
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
				map[string]int{"virtual-storage-1": tc.replicationFactor},
			).RouteRepositoryCreation(ctx, tc.virtualStorage)
			require.Equal(t, tc.error, err)

			// assert replication targets separately as the picked secondary
			// is random
			tc.requireReplicationTargets(t, route.ReplicationTargets)
			route.ReplicationTargets = nil

			require.Equal(t, RepositoryMutatorRoute{
				Primary: RouterNode{Storage: tc.primary, Connection: conns[tc.virtualStorage][tc.primary]},
			}, route)
		})
	}
}
