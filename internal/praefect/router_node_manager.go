package praefect

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"
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

func (r *nodeManagerRouter) RouteRepositoryAccessor(ctx context.Context, virtualStorage, relativePath string) (RouterNode, error) {
	node, err := r.mgr.GetSyncedNode(ctx, virtualStorage, relativePath)
	if err != nil {
		return RouterNode{}, fmt.Errorf("get synced node: %w", err)
	}

	return toRouterNode(node), nil
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

func (r *nodeManagerRouter) RouteRepositoryMutator(ctx context.Context, virtualStorage, relativePath string) (RepositoryMutatorRoute, error) {
	shard, err := r.mgr.GetShard(ctx, virtualStorage)
	if err != nil {
		return RepositoryMutatorRoute{}, fmt.Errorf("get shard: %w", err)
	}

	consistentStorages, err := r.rs.GetConsistentStorages(ctx, virtualStorage, relativePath)
	if err != nil {
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
		Primary:            toRouterNode(shard.Primary),
		Secondaries:        toRouterNodes(participatingSecondaries),
		ReplicationTargets: replicationTargets,
	}, nil
}

func (r *nodeManagerRouter) RouteRepositoryCreation(ctx context.Context, virtualStorage string) (RepositoryMutatorRoute, error) {
	// nodeManagerRouter doesn't support repository assignments nor repository specific primaries. It
	// is sufficient to route the requests as normal mutators.
	return r.RouteRepositoryMutator(ctx, virtualStorage, "")
}
