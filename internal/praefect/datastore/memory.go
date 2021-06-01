package datastore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
)

var (
	errDeadAckedAsFailed = errors.New("job acknowledged as failed with no attempts left, should be 'dead'")
)

// NewMemoryReplicationEventQueue return in-memory implementation of the ReplicationEventQueue.
func NewMemoryReplicationEventQueue(conf config.Config) ReplicationEventQueue {
	storageNamesByVirtualStorage := make(map[string][]string, len(conf.VirtualStorages))
	for _, vs := range conf.VirtualStorages {
		storages := make([]string, len(vs.Nodes))
		for i, node := range vs.Nodes {
			storages[i] = node.Storage
		}
		storageNamesByVirtualStorage[vs.Name] = storages
	}
	return &memoryReplicationEventQueue{
		dequeued:                     map[uint64]struct{}{},
		storageNamesByVirtualStorage: storageNamesByVirtualStorage,
		lastEventByDest:              map[eventDestination]ReplicationEvent{},
	}
}

type eventDestination struct {
	virtual, storage, relativePath string
}

// memoryReplicationEventQueue implements queue interface with in-memory implementation of storage
type memoryReplicationEventQueue struct {
	sync.RWMutex
	seq                          uint64                                // used to generate unique  identifiers for events
	queued                       []ReplicationEvent                    // all new events stored as queue
	dequeued                     map[uint64]struct{}                   // all events dequeued, but not yet acknowledged
	storageNamesByVirtualStorage map[string][]string                   // bindings between virtual storage and storages behind them
	lastEventByDest              map[eventDestination]ReplicationEvent // contains 'virtual+storage+repo' => 'last even' mappings
}

// nextID returns a new sequential ID for new events.
// Needs to be called with lock protection.
func (s *memoryReplicationEventQueue) nextID() uint64 {
	s.seq++
	return s.seq
}

func (s *memoryReplicationEventQueue) Enqueue(_ context.Context, event ReplicationEvent) (ReplicationEvent, error) {
	event.Attempt = 3
	event.State = JobStateReady
	event.CreatedAt = time.Now().UTC()
	// event.LockID is unnecessary with an in memory data store as it is intended to synchronize multiple praefect instances
	// but must be filled out to produce same event as it done by SQL implementation
	event.LockID = event.Job.VirtualStorage + "|" + event.Job.TargetNodeStorage + "|" + event.Job.RelativePath
	dest := s.defineDest(event)

	s.Lock()
	defer s.Unlock()
	event.ID = s.nextID()
	s.queued = append(s.queued, event)
	s.lastEventByDest[dest] = event
	return event, nil
}

func (s *memoryReplicationEventQueue) Dequeue(_ context.Context, virtualStorage, nodeStorage string, count int) ([]ReplicationEvent, error) {
	s.Lock()
	defer s.Unlock()

	var result []ReplicationEvent
	uniqueJob := make(map[string]struct{})

	for i := 0; i < len(s.queued); i++ {
		event := s.queued[i]

		isForVirtualStorage := event.Job.VirtualStorage == virtualStorage
		isForTargetStorage := event.Job.TargetNodeStorage == nodeStorage
		isReadyOrFailed := event.State == JobStateReady || event.State == JobStateFailed

		if isForVirtualStorage && isForTargetStorage && isReadyOrFailed {
			jobData, err := json.Marshal(event.Job)
			if err != nil {
				return nil, err
			}

			if _, found := uniqueJob[string(jobData)]; found {
				continue
			}

			uniqueJob[string(jobData)] = struct{}{}

			updatedAt := time.Now().UTC()
			event.Attempt--
			event.State = JobStateInProgress
			event.UpdatedAt = &updatedAt

			s.queued[i] = event
			s.dequeued[event.ID] = struct{}{}
			eventDest := s.defineDest(event)
			if last, found := s.lastEventByDest[eventDest]; found && last.ID == event.ID {
				s.lastEventByDest[eventDest] = event
			}
			result = append(result, event)

			if len(result) >= count {
				break
			}
		}
	}

	return result, nil
}

func (s *memoryReplicationEventQueue) Acknowledge(_ context.Context, state JobState, ids []uint64) ([]uint64, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	if err := allowToAck(state); err != nil {
		return nil, err
	}

	s.Lock()
	defer s.Unlock()

	var result []uint64
	for _, id := range ids {
		if _, found := s.dequeued[id]; !found {
			// event was not dequeued from the queue, so it can't be acknowledged
			continue
		}

		for i := 0; i < len(s.queued); i++ {
			if s.queued[i].ID != id {
				continue
			}

			if s.queued[i].State != JobStateInProgress {
				return nil, fmt.Errorf("event not in progress, can't be acknowledged: %d [%s]", s.queued[i].ID, s.queued[i].State)
			}

			if s.queued[i].Attempt == 0 && state == JobStateFailed {
				return nil, errDeadAckedAsFailed
			}

			dequeuedAt := s.queued[i].UpdatedAt
			updatedAt := time.Now().UTC()
			s.queued[i].State = state
			s.queued[i].UpdatedAt = &updatedAt
			eventDest := s.defineDest(s.queued[i])
			if last, found := s.lastEventByDest[eventDest]; found && last.ID == s.queued[i].ID {
				s.lastEventByDest[eventDest] = s.queued[i]
			}
			result = append(result, id)

			if state == JobStateCompleted {
				ackJobData, err := json.Marshal(s.queued[i].Job)
				if err != nil {
					return nil, err
				}

				for j := i + 1; j < len(s.queued); j++ {
					if dequeuedAt.Before(s.queued[j].CreatedAt) {
						break
					}

					sameJobData, err := json.Marshal(s.queued[j].Job)
					if err != nil {
						return nil, err
					}

					if bytes.Equal(ackJobData, sameJobData) {
						s.remove(j)
					}
				}
			}

			switch state {
			case JobStateCompleted, JobStateCancelled, JobStateDead:
				// this event is fully processed and could be removed
				s.remove(i)
			}
			break
		}
	}

	return result, nil
}

// StartHealthUpdate does nothing as it has no sense in terms of in-memory implementation as
// all information about events will be lost after restart.
func (s *memoryReplicationEventQueue) StartHealthUpdate(context.Context, <-chan time.Time, []ReplicationEvent) error {
	return nil
}

func (s *memoryReplicationEventQueue) AcknowledgeStale(context.Context, time.Duration) error {
	// this implementation has no problem of stale replication events as it has no information about
	// job processing after restart of the application
	return nil
}

// remove deletes i-th element from the queue and from the in-flight tracking map.
// It doesn't check 'i' for the out of range and must be called with lock protection.
func (s *memoryReplicationEventQueue) remove(i int) {
	delete(s.dequeued, s.queued[i].ID)
	s.queued = append(s.queued[:i], s.queued[i+1:]...)
}

func (s *memoryReplicationEventQueue) defineDest(event ReplicationEvent) eventDestination {
	return eventDestination{virtual: event.Job.VirtualStorage, storage: event.Job.TargetNodeStorage, relativePath: event.Job.RelativePath}
}

// ReplicationEventQueueInterceptor allows to register interceptors for `ReplicationEventQueue` interface.
type ReplicationEventQueueInterceptor interface {
	// ReplicationEventQueue actual implementation.
	ReplicationEventQueue
	// OnEnqueue allows to set action that would be executed each time when `Enqueue` method called.
	OnEnqueue(func(context.Context, ReplicationEvent, ReplicationEventQueue) (ReplicationEvent, error))
	// OnDequeue allows to set action that would be executed each time when `Dequeue` method called.
	OnDequeue(func(context.Context, string, string, int, ReplicationEventQueue) ([]ReplicationEvent, error))
	// OnAcknowledge allows to set action that would be executed each time when `Acknowledge` method called.
	OnAcknowledge(func(context.Context, JobState, []uint64, ReplicationEventQueue) ([]uint64, error))
	// OnStartHealthUpdate allows to set action that would be executed each time when `StartHealthUpdate` method called.
	OnStartHealthUpdate(func(context.Context, <-chan time.Time, []ReplicationEvent) error)
	// OnAcknowledgeStale allows to set action that would be executed each time when `AcknowledgeStale` method called.
	OnAcknowledgeStale(func(context.Context, time.Duration) error)
}

// NewReplicationEventQueueInterceptor returns interception over `ReplicationEventQueue` interface.
func NewReplicationEventQueueInterceptor(queue ReplicationEventQueue) ReplicationEventQueueInterceptor {
	return &replicationEventQueueInterceptor{ReplicationEventQueue: queue}
}

type replicationEventQueueInterceptor struct {
	ReplicationEventQueue
	onEnqueue           func(context.Context, ReplicationEvent, ReplicationEventQueue) (ReplicationEvent, error)
	onDequeue           func(context.Context, string, string, int, ReplicationEventQueue) ([]ReplicationEvent, error)
	onAcknowledge       func(context.Context, JobState, []uint64, ReplicationEventQueue) ([]uint64, error)
	onStartHealthUpdate func(context.Context, <-chan time.Time, []ReplicationEvent) error
	onAcknowledgeStale  func(context.Context, time.Duration) error
}

func (i *replicationEventQueueInterceptor) OnEnqueue(action func(context.Context, ReplicationEvent, ReplicationEventQueue) (ReplicationEvent, error)) {
	i.onEnqueue = action
}

func (i *replicationEventQueueInterceptor) OnDequeue(action func(context.Context, string, string, int, ReplicationEventQueue) ([]ReplicationEvent, error)) {
	i.onDequeue = action
}

func (i *replicationEventQueueInterceptor) OnAcknowledge(action func(context.Context, JobState, []uint64, ReplicationEventQueue) ([]uint64, error)) {
	i.onAcknowledge = action
}

func (i *replicationEventQueueInterceptor) OnStartHealthUpdate(action func(context.Context, <-chan time.Time, []ReplicationEvent) error) {
	i.onStartHealthUpdate = action
}

func (i *replicationEventQueueInterceptor) OnAcknowledgeStale(action func(context.Context, time.Duration) error) {
	i.onAcknowledgeStale = action
}

func (i *replicationEventQueueInterceptor) Enqueue(ctx context.Context, event ReplicationEvent) (ReplicationEvent, error) {
	if i.onEnqueue != nil {
		return i.onEnqueue(ctx, event, i.ReplicationEventQueue)
	}
	return i.ReplicationEventQueue.Enqueue(ctx, event)
}

func (i *replicationEventQueueInterceptor) Dequeue(ctx context.Context, virtualStorage, nodeStorage string, count int) ([]ReplicationEvent, error) {
	if i.onDequeue != nil {
		return i.onDequeue(ctx, virtualStorage, nodeStorage, count, i.ReplicationEventQueue)
	}
	return i.ReplicationEventQueue.Dequeue(ctx, virtualStorage, nodeStorage, count)
}

func (i *replicationEventQueueInterceptor) Acknowledge(ctx context.Context, state JobState, ids []uint64) ([]uint64, error) {
	if i.onAcknowledge != nil {
		return i.onAcknowledge(ctx, state, ids, i.ReplicationEventQueue)
	}
	return i.ReplicationEventQueue.Acknowledge(ctx, state, ids)
}

func (i *replicationEventQueueInterceptor) StartHealthUpdate(ctx context.Context, trigger <-chan time.Time, events []ReplicationEvent) error {
	if i.onStartHealthUpdate != nil {
		return i.onStartHealthUpdate(ctx, trigger, events)
	}
	return i.ReplicationEventQueue.StartHealthUpdate(ctx, trigger, events)
}

func (i *replicationEventQueueInterceptor) AcknowledgeStale(ctx context.Context, staleAfter time.Duration) error {
	if i.onAcknowledgeStale != nil {
		return i.onAcknowledgeStale(ctx, staleAfter)
	}
	return i.ReplicationEventQueue.AcknowledgeStale(ctx, staleAfter)
}
