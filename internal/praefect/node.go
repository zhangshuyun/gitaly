package praefect

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/nodes/tracker"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/protoregistry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// Node is a storage node in a virtual storage.
type Node struct {
	// Storage is the name of the storage node.
	Storage string
	// Address is the address of the node.
	Address string
	// Token is the authentication token of the node.
	Token string
	// Connection is a gRPC connection to the storage node.
	Connection *grpc.ClientConn
}

// NodeSet contains nodes by their virtual storage and storage names.
type NodeSet map[string]map[string]Node

// Close closes the connections in the NodeSet. Errors on closing are ignored.
func (set NodeSet) Close() {
	for _, nodes := range set {
		for _, node := range nodes {
			node.Connection.Close()
		}
	}
}

// HealthClients is a convenience method to return the HealthClients from the NodeSet.
func (set NodeSet) HealthClients() nodes.HealthClients {
	clients := make(nodes.HealthClients, len(set))
	for virtualStorage, nodes := range set {
		clients[virtualStorage] = make(map[string]grpc_health_v1.HealthClient, len(nodes))
		for _, node := range nodes {
			clients[virtualStorage][node.Storage] = grpc_health_v1.NewHealthClient(node.Connection)
		}
	}

	return clients
}

// Connections is a convenience method to return the connections from the NodeSet.
func (set NodeSet) Connections() Connections {
	conns := make(Connections, len(set))
	for virtualStorage, nodes := range set {
		conns[virtualStorage] = make(map[string]*grpc.ClientConn, len(nodes))
		for _, node := range nodes {
			conns[virtualStorage][node.Storage] = node.Connection
		}
	}

	return conns
}

// NodeSetFromNodeManager converts connections set up by the node manager
// in to a NodeSet. This is a temporary adapter required due to cyclic
// imports between the praefect and nodes packages.
func NodeSetFromNodeManager(mgr nodes.Manager) NodeSet {
	nodes := mgr.Nodes()

	set := make(NodeSet, len(nodes))
	for virtualStorage, nodes := range nodes {
		set[virtualStorage] = make(map[string]Node, len(nodes))
		for _, node := range nodes {
			set[virtualStorage][node.GetStorage()] = toNode(node)
		}
	}

	return set
}

func toNode(node nodes.Node) Node {
	return Node{
		Storage:    node.GetStorage(),
		Address:    node.GetAddress(),
		Token:      node.GetToken(),
		Connection: node.GetConnection(),
	}
}

// DialNodes dials the configured storage nodes.
func DialNodes(
	ctx context.Context,
	virtualStorages []*config.VirtualStorage,
	registry *protoregistry.Registry,
	errorTracker tracker.ErrorTracker,
	handshaker client.Handshaker,
) (NodeSet, error) {
	set := make(NodeSet, len(virtualStorages))
	for _, virtualStorage := range virtualStorages {
		set[virtualStorage.Name] = make(map[string]Node, len(virtualStorage.Nodes))
		for _, node := range virtualStorage.Nodes {
			conn, err := nodes.Dial(ctx, node, registry, errorTracker, handshaker)
			if err != nil {
				return nil, fmt.Errorf("dial %q/%q: %w", virtualStorage.Name, node.Storage, err)
			}

			set[virtualStorage.Name][node.Storage] = Node{
				Storage:    node.Storage,
				Address:    node.Address,
				Token:      node.Token,
				Connection: conn,
			}
		}
	}

	return set, nil
}
