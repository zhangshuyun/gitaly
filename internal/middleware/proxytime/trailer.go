package proxytime

import (
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// TrailerTracker is an interface that tracks trailers to allow other components to access grpc trailer metaata.
type TrailerTracker interface {
	Trailer(id string) grpc.CallOption
	RetrieveTrailer(id string) *metadata.MD
}

// Tracker is an implementation of TrailerTracker
type Tracker struct {
	mutex    sync.RWMutex
	trailers map[string]*metadata.MD
}

// NewTrailerTracker creates a new metrics struct
func NewTrailerTracker() *Tracker {
	return &Tracker{
		trailers: make(map[string]*metadata.MD),
	}
}

// Trailer gets the gRPC trailer for a given id
func (t *Tracker) Trailer(id string) grpc.CallOption {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	if trailer, ok := t.trailers[id]; ok {
		return grpc.Trailer(trailer)
	}

	newTrailer := make(metadata.MD)
	t.trailers[id] = &newTrailer

	return grpc.Trailer(&newTrailer)
}

// RetrieveTrailer deletes the trailer after retrieving it based on the id
func (t *Tracker) RetrieveTrailer(id string) *metadata.MD {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	defer delete(t.trailers, id)

	if header, ok := t.trailers[id]; ok {
		return header
	}

	newHeader := make(metadata.MD)
	return &newHeader
}
