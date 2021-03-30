package backchannel

import (
	"fmt"
	"sync"

	"google.golang.org/grpc"
)

// ID is a monotonically increasing number that uniquely identifies a peer connection.
type ID uint64

// Registry is a thread safe registry for backchannels. It enables accessing the backchannels via a
// unique ID.
type Registry struct {
	m            sync.RWMutex
	currentID    ID
	backchannels map[ID]*grpc.ClientConn
}

// NewRegistry returns a new Registry.
func NewRegistry() *Registry { return &Registry{backchannels: map[ID]*grpc.ClientConn{}} }

// Backchannel returns a backchannel for the ID. Returns an error if no backchannel is registered
// for the ID.
func (r *Registry) Backchannel(id ID) (*grpc.ClientConn, error) {
	r.m.RLock()
	defer r.m.RUnlock()
	backchannel, ok := r.backchannels[id]
	if !ok {
		return nil, fmt.Errorf("no backchannel for peer %d", id)
	}

	return backchannel, nil
}

// RegisterBackchannel registers a new backchannel and returns its unique ID.
func (r *Registry) RegisterBackchannel(conn *grpc.ClientConn) ID {
	r.m.Lock()
	defer r.m.Unlock()
	r.currentID++
	r.backchannels[r.currentID] = conn
	return r.currentID
}

// RemoveBackchannel removes a backchannel from the registry.
func (r *Registry) RemoveBackchannel(id ID) {
	r.m.Lock()
	defer r.m.Unlock()
	delete(r.backchannels, id)
}
