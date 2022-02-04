package praefect

import (
	"context"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/commonerr"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/nodes"
)

type nodeManagerRouter struct {
	mgr nodes.Manager
	rs  datastore.RepositoryStore
}

func toRouterNode(node nodes.Node) RouterNode {
	return RouterNode{
		Storage:    node.GetStorage(),
		Connection: node.GetConnection(),
	}
}

func toRouterNodes(nodes []nodes.Node) []RouterNode {
	out := make([]RouterNode, len(nodes))
	for i := range nodes {
		out[i] = toRouterNode(nodes[i])
	}
	return out
}

// NewNodeManagerRouter returns a router that uses the NodeManager to make routing decisions.
func NewNodeManagerRouter(mgr nodes.Manager, rs datastore.RepositoryStore) Router {
	return &nodeManagerRouter{mgr: mgr, rs: rs}
}

func (r *nodeManagerRouter) RouteRepositoryAccessor(ctx context.Context, virtualStorage, relativePath string, forcePrimary bool) (RepositoryAccessorRoute, error) {
	if forcePrimary {
		shard, err := r.mgr.GetShard(ctx, virtualStorage)
		if err != nil {
			return RepositoryAccessorRoute{}, fmt.Errorf("get shard: %w", err)
		}

		return RepositoryAccessorRoute{ReplicaPath: relativePath, Node: toRouterNode(shard.Primary)}, nil
	}

	node, err := r.mgr.GetSyncedNode(ctx, virtualStorage, relativePath)
	if err != nil {
		return RepositoryAccessorRoute{}, fmt.Errorf("get synced node: %w", err)
	}

	return RepositoryAccessorRoute{ReplicaPath: relativePath, Node: toRouterNode(node)}, nil
}

func (r *nodeManagerRouter) RouteStorageAccessor(ctx context.Context, virtualStorage string) (RouterNode, error) {
	shard, err := r.mgr.GetShard(ctx, virtualStorage)
	if err != nil {
		return RouterNode{}, err
	}

	return toRouterNode(shard.Primary), nil
}

func (r *nodeManagerRouter) RouteStorageMutator(ctx context.Context, virtualStorage string) (StorageMutatorRoute, error) {
	shard, err := r.mgr.GetShard(ctx, virtualStorage)
	if err != nil {
		return StorageMutatorRoute{}, err
	}

	return StorageMutatorRoute{
		Primary:     toRouterNode(shard.Primary),
		Secondaries: toRouterNodes(shard.GetHealthySecondaries()),
	}, nil
}

func (r *nodeManagerRouter) RouteRepositoryMutator(ctx context.Context, virtualStorage, relativePath, additionalRelativePath string) (RepositoryMutatorRoute, error) {
	shard, err := r.mgr.GetShard(ctx, virtualStorage)
	if err != nil {
		return RepositoryMutatorRoute{}, fmt.Errorf("get shard: %w", err)
	}

	// The replica path is ignored as Rails' tests are the only user of NodeManagerRouter. The tests don't
	// set up a database, so the RepositoryStore here is always a mock. The mock doesn't know about the replica
	// paths of repositories and thus returns an empty string. This breaks the tests. Instead, we'll just keep
	// using the relative path in NodeManagerRouter.
	_, consistentStorages, err := r.rs.GetConsistentStorages(ctx, virtualStorage, relativePath)
	if err != nil && !errors.As(err, new(commonerr.RepositoryNotFoundError)) {
		return RepositoryMutatorRoute{}, fmt.Errorf("consistent storages: %w", err)
	}

	if len(consistentStorages) == 0 {
		// if there is no up to date storages we'll have to consider the storage
		// up to date as this will be the case on repository creation
		consistentStorages = map[string]struct{}{shard.Primary.GetStorage(): {}}
	}

	if _, ok := consistentStorages[shard.Primary.GetStorage()]; !ok {
		return RepositoryMutatorRoute{}, ErrRepositoryReadOnly
	}

	// Inconsistent nodes will anyway need repair so including them doesn't make sense. They
	// also might vote to abort which might unnecessarily fail the transaction.
	var replicationTargets []string
	// Only healthy secondaries which are consistent with the primary are allowed to take
	// part in the transaction. Unhealthy nodes would block the transaction until they come back.
	participatingSecondaries := make([]nodes.Node, 0, len(consistentStorages))
	for _, secondary := range shard.Secondaries {
		if _, ok := consistentStorages[secondary.GetStorage()]; ok && secondary.IsHealthy() {
			participatingSecondaries = append(participatingSecondaries, secondary)
			continue
		}

		replicationTargets = append(replicationTargets, secondary.GetStorage())
	}

	return RepositoryMutatorRoute{
		ReplicaPath:           relativePath,
		AdditionalReplicaPath: additionalRelativePath,
		Primary:               toRouterNode(shard.Primary),
		Secondaries:           toRouterNodes(participatingSecondaries),
		ReplicationTargets:    replicationTargets,
	}, nil
}

// RouteRepositoryCreation includes healthy secondaries in the transaction and sets the unhealthy secondaries as
// replication targets. The virtual storage's primary acts as the primary for every repository.
func (r *nodeManagerRouter) RouteRepositoryCreation(ctx context.Context, virtualStorage, relativePath, additionalRepoRelativePath string) (RepositoryMutatorRoute, error) {
	shard, err := r.mgr.GetShard(ctx, virtualStorage)
	if err != nil {
		return RepositoryMutatorRoute{}, fmt.Errorf("get shard: %w", err)
	}

	var secondaries []RouterNode
	var replicationTargets []string

	for _, secondary := range shard.Secondaries {
		if secondary.IsHealthy() {
			secondaries = append(secondaries, toRouterNode(secondary))
			continue
		}

		replicationTargets = append(replicationTargets, secondary.GetStorage())
	}

	return RepositoryMutatorRoute{
		Primary:               toRouterNode(shard.Primary),
		ReplicaPath:           relativePath,
		AdditionalReplicaPath: additionalRepoRelativePath,
		Secondaries:           secondaries,
		ReplicationTargets:    replicationTargets,
	}, nil
}
