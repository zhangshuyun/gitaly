package proxytime

import (
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// TrailerTracker is an interface that tracks trailers to allow other components to access grpc trailer metadata.
type TrailerTracker interface {
	Trailer(id string) (grpc.CallOption, error)
	RemoveTrailer(id string) (*metadata.MD, error)
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

// Trailer returns a call option that will set the gRPC trailer for a given id.
func (t *Tracker) Trailer(id string) (grpc.CallOption, error) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	if _, ok := t.trailers[id]; ok {
		return nil, errors.New("a trailer with the same id is already in flight")
	}

	newTrailer := make(metadata.MD)
	t.trailers[id] = &newTrailer

	go func() {
		<-time.NewTimer(5 * time.Minute).C
		t.RemoveTrailer(id)
	}()

	return grpc.Trailer(&newTrailer), nil
}

// RemoveTrailer deletes the trailer after retrieving it based on the id
func (t *Tracker) RemoveTrailer(id string) (*metadata.MD, error) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	if trailer, ok := t.trailers[id]; ok {
		delete(t.trailers, id)
		return trailer, nil
	}

	return nil, errors.New("trailer with does not exist")
}
