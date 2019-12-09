package proxytime

import (
	"errors"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// MaxTrailers is the cap for number of trailers the trailer tracker can track
const MaxTrailers = 100000

// TrailerTracker is an interface that tracks trailers to allow other components to access grpc trailer metadata.
type TrailerTracker struct {
	mutex    sync.Mutex
	trailers map[string]*metadata.MD
}

// NewTrailerTracker creates a new metrics struct
func NewTrailerTracker() *TrailerTracker {
	return &TrailerTracker{
		trailers: make(map[string]*metadata.MD),
	}
}

// Trailer returns a call option that will set the gRPC trailer for a given id.
func (t *TrailerTracker) Trailer(id string) (grpc.CallOption, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if len(t.trailers) > MaxTrailers {
		return nil, errors.New("maximum number of trailers reached")
	}

	if _, ok := t.trailers[id]; ok {
		return nil, errors.New("a trailer with the same id is already in flight")
	}

	newTrailer := metadata.New(nil)
	t.trailers[id] = &newTrailer

	return grpc.Trailer(&newTrailer), nil
}

// RemoveTrailer deletes the trailer after retrieving it based on the id
func (t *TrailerTracker) RemoveTrailer(id string) (*metadata.MD, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if trailer, ok := t.trailers[id]; ok {
		delete(t.trailers, id)
		return trailer, nil
	}

	return nil, fmt.Errorf("trailer with request_id %s does not exist", id)
}
