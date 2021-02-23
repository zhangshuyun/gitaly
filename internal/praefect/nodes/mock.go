package nodes

import (
	"context"

	"google.golang.org/grpc"
)

// MockManager is a helper for tests that implements Manager and allows
// for parametrizing behavior.
type MockManager struct {
	Manager
	GetShardFunc func(string) (Shard, error)
	Storage      string
}

func (m *MockManager) GetShard(_ context.Context, storage string) (Shard, error) {
	return m.GetShardFunc(storage)
}

// Nodes returns nodes contained by the GetShardFunc. Note that this mocking only works in case the
// MockManager was set up with a Storage as the GetShardFunc will be called with that storage as
// parameter.
func (m *MockManager) Nodes() map[string][]Node {
	nodes := make(map[string][]Node)
	shard, _ := m.GetShardFunc(m.Storage)
	nodes[m.Storage] = append(nodes[m.Storage], shard.Primary)
	nodes[m.Storage] = append(nodes[m.Storage], shard.Secondaries...)
	return nodes
}

// HealthyNodes returns healthy nodes. This is implemented similar to Nodes() and thus also requires
// setup of the MockManager's Storage field.
func (m *MockManager) HealthyNodes() map[string][]string {
	shard, _ := m.GetShardFunc(m.Storage)

	nodes := make(map[string][]string)
	if shard.Primary.IsHealthy() {
		nodes[m.Storage] = append(nodes[m.Storage], shard.Primary.GetStorage())
	}

	for _, secondary := range shard.Secondaries {
		if secondary.IsHealthy() {
			nodes[m.Storage] = append(nodes[m.Storage], secondary.GetStorage())
		}
	}

	return nodes
}

// MockNode is a helper for tests that implements Node and allows
// for parametrizing behavior.
type MockNode struct {
	Node
	GetStorageMethod func() string
	Conn             *grpc.ClientConn
	Healthy          bool
}

func (m *MockNode) GetStorage() string { return m.GetStorageMethod() }

func (m *MockNode) IsHealthy() bool { return m.Healthy }

func (m *MockNode) GetConnection() *grpc.ClientConn { return m.Conn }

func (m *MockNode) GetAddress() string { return "" }

func (m *MockNode) GetToken() string { return "" }
