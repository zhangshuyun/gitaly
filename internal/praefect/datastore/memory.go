package datastore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
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
		maxDeadJobs:                  1000,
		storageNamesByVirtualStorage: storageNamesByVirtualStorage,
		lastEventByDest:              map[eventDestination]ReplicationEvent{},
	}
}

type deadJob struct {
	createdAt    time.Time
	relativePath string
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
	maxDeadJobs                  int                                   // maximum amount of dead jobs to hold in memory
	deadJobs                     []deadJob                             // dead jobs stored for reporting purposes
	storageNamesByVirtualStorage map[string][]string                   // bindings between virtual storage and storages behind them
	lastEventByDest              map[eventDestination]ReplicationEvent // contains 'virtual+storage+repo' => 'last even' mappings
}

// nextID returns a new sequential ID for new events.
// Needs to be called with lock protection.
func (s *memoryReplicationEventQueue) nextID() uint64 {
	s.seq++
	return s.seq
}

func (s *memoryReplicationEventQueue) GetOutdatedRepositories(ctx context.Context, virtualStorage string, referenceStorage string) (map[string][]string, error) {
	return nil, errors.New("unimplemented")
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
	for i := 0; i < len(s.queued); i++ {
		event := s.queued[i]

		isForVirtualStorage := event.Job.VirtualStorage == virtualStorage
		isForTargetStorage := event.Job.TargetNodeStorage == nodeStorage
		isReadyOrFailed := event.State == JobStateReady || event.State == JobStateFailed

		if isForVirtualStorage && isForTargetStorage && isReadyOrFailed {
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

			updatedAt := time.Now().UTC()
			s.queued[i].State = state
			s.queued[i].UpdatedAt = &updatedAt
			eventDest := s.defineDest(s.queued[i])
			if last, found := s.lastEventByDest[eventDest]; found && last.ID == s.queued[i].ID {
				s.lastEventByDest[eventDest] = s.queued[i]
			}
			result = append(result, id)

			switch state {
			case JobStateCompleted, JobStateCancelled, JobStateDead:
				// this event is fully processed and could be removed
				s.remove(i, state)
			}
			break
		}
	}

	return result, nil
}

// CountDeadReplicationJobs returns the dead replication job counts by relative path within the given timerange.
// The timerange beginning is inclusive and ending is exclusive. The in-memory queue stores only the most recent
// 1000 dead jobs.
func (s *memoryReplicationEventQueue) CountDeadReplicationJobs(ctx context.Context, from, to time.Time) (map[string]int64, error) {
	s.RLock()
	defer s.RUnlock()

	from = from.Add(-time.Nanosecond)
	dead := map[string]int64{}
	for _, job := range s.deadJobs {
		if job.createdAt.After(from) && job.createdAt.Before(to) {
			dead[job.relativePath]++
		}
	}

	return dead, nil
}

func (s *memoryReplicationEventQueue) GetUpToDateStorages(_ context.Context, virtualStorage, repoPath string) ([]string, error) {
	s.RLock()
	dirtyStorages := make(map[string]struct{})
	for dst, event := range s.lastEventByDest {
		if dst.virtual == virtualStorage && dst.relativePath == repoPath && event.State != JobStateCompleted {
			dirtyStorages[event.Job.TargetNodeStorage] = struct{}{}
		}
	}
	s.RUnlock()

	storageNames, found := s.storageNamesByVirtualStorage[virtualStorage]
	if !found {
		return nil, nil
	}

	var result []string
	for _, storage := range storageNames {
		if _, found := dirtyStorages[storage]; !found {
			result = append(result, storage)
		}
	}
	return result, nil
}

// remove deletes i-th element from the queue and from the in-flight tracking map.
// It doesn't check 'i' for the out of range and must be called with lock protection.
// If state is JobStateDead, the event will be added to the dead job tracker.
func (s *memoryReplicationEventQueue) remove(i int, state JobState) {
	if state == JobStateDead {
		if len(s.deadJobs) >= s.maxDeadJobs {
			s.deadJobs = s.deadJobs[1:]
		}

		s.deadJobs = append(s.deadJobs, deadJob{
			createdAt:    s.queued[i].CreatedAt,
			relativePath: s.queued[i].Job.RelativePath,
		})
	}

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
}

// NewReplicationEventQueueInterceptor returns interception over `ReplicationEventQueue` interface.
func NewReplicationEventQueueInterceptor(queue ReplicationEventQueue) ReplicationEventQueueInterceptor {
	return &replicationEventQueueInterceptor{ReplicationEventQueue: queue}
}

type replicationEventQueueInterceptor struct {
	ReplicationEventQueue
	onEnqueue     func(context.Context, ReplicationEvent, ReplicationEventQueue) (ReplicationEvent, error)
	onDequeue     func(context.Context, string, string, int, ReplicationEventQueue) ([]ReplicationEvent, error)
	onAcknowledge func(context.Context, JobState, []uint64, ReplicationEventQueue) ([]uint64, error)
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
