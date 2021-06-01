package datastore

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestMemoryReplicationEventQueue(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	queue := NewMemoryReplicationEventQueue(config.Config{})

	noEvents, err := queue.Dequeue(ctx, "praefect", "storage-1", 100500)
	require.NoError(t, err)
	require.Empty(t, noEvents, "no events as queue is empty")

	noAcknowledged, err := queue.Acknowledge(ctx, JobStateCompleted, []uint64{1, 2, 3})
	require.NoError(t, err)
	require.Empty(t, noAcknowledged, "no acknowledged ids as queue is empty")

	job1 := ReplicationJob{
		Change:            UpdateRepo,
		RelativePath:      "/project/path-1",
		VirtualStorage:    "praefect",
		TargetNodeStorage: "storage-1",
		SourceNodeStorage: "storage-0",
		Params:            nil,
	}

	eventType1 := ReplicationEvent{Job: job1}

	event1, err := queue.Enqueue(ctx, eventType1)
	require.NoError(t, err)

	expEvent1 := ReplicationEvent{
		ID:        1,
		State:     JobStateReady,
		Attempt:   3,
		LockID:    "praefect|storage-1|/project/path-1",
		CreatedAt: event1.CreatedAt, // it is a hack to have same time for both
		Job:       job1,
	}
	require.Equal(t, expEvent1, event1)

	notAcknowledged1, err := queue.Acknowledge(ctx, JobStateCompleted, []uint64{event1.ID})
	require.NoError(t, err)
	require.Empty(t, notAcknowledged1, "no acknowledged ids as events were not dequeued")

	job2 := ReplicationJob{
		Change:            UpdateRepo,
		RelativePath:      "/project/path-1",
		VirtualStorage:    "praefect",
		TargetNodeStorage: "storage-2",
		SourceNodeStorage: "storage-0",
		Params:            nil,
	}
	eventType2 := ReplicationEvent{Job: job2}

	event2, err := queue.Enqueue(ctx, eventType2)
	require.NoError(t, err)

	expEvent2 := ReplicationEvent{
		ID:        2,
		State:     JobStateReady,
		Attempt:   3,
		LockID:    "praefect|storage-2|/project/path-1",
		CreatedAt: event2.CreatedAt, // it is a hack to have same time for both
		Job:       job2,
	}
	require.Equal(t, expEvent2, event2)

	dequeuedAttempt1, err := queue.Dequeue(ctx, "praefect", "storage-1", 100500)
	require.NoError(t, err)
	require.Len(t, dequeuedAttempt1, 1, "only single event must be fetched for this storage")

	expAttempt1 := ReplicationEvent{
		ID:        1,
		State:     JobStateInProgress,
		Attempt:   2,
		LockID:    "praefect|storage-1|/project/path-1",
		CreatedAt: event1.CreatedAt,              // it is a hack to have same time for both
		UpdatedAt: dequeuedAttempt1[0].UpdatedAt, // it is a hack to have same time for both
		Job:       job1,
	}
	require.Equal(t, expAttempt1, dequeuedAttempt1[0])

	acknowledgedAttempt1, err := queue.Acknowledge(ctx, JobStateFailed, []uint64{event1.ID, event2.ID})
	require.NoError(t, err)
	require.Equal(t, []uint64{event1.ID}, acknowledgedAttempt1, "one event must be acknowledged")

	dequeuedAttempt2, err := queue.Dequeue(ctx, "praefect", "storage-1", 100500)
	require.NoError(t, err)
	require.Len(t, dequeuedAttempt2, 1, "only single event must be fetched for this storage")

	expAttempt2 := ReplicationEvent{
		ID:        1,
		State:     JobStateInProgress,
		Attempt:   1,
		LockID:    "praefect|storage-1|/project/path-1",
		CreatedAt: event1.CreatedAt,              // it is a hack to have same time for both
		UpdatedAt: dequeuedAttempt2[0].UpdatedAt, // it is a hack to have same time for both
		Job:       job1,
	}
	require.Equal(t, expAttempt2, dequeuedAttempt2[0])

	acknowledgedAttempt2, err := queue.Acknowledge(ctx, JobStateFailed, []uint64{event1.ID})
	require.NoError(t, err)
	require.Equal(t, []uint64{event1.ID}, acknowledgedAttempt2, "one event must be acknowledged")

	dequeuedAttempt3, err := queue.Dequeue(ctx, "praefect", "storage-1", 100500)
	require.NoError(t, err)
	require.Len(t, dequeuedAttempt3, 1, "only single event must be fetched for this storage")

	expAttempt3 := ReplicationEvent{
		ID:        1,
		State:     JobStateInProgress,
		Attempt:   0,
		LockID:    "praefect|storage-1|/project/path-1",
		CreatedAt: event1.CreatedAt,              // it is a hack to have same time for both
		UpdatedAt: dequeuedAttempt3[0].UpdatedAt, // it is a hack to have same time for both
		Job:       job1,
	}
	require.Equal(t, expAttempt3, dequeuedAttempt3[0])

	ackFailedNoAttemptsLeft, err := queue.Acknowledge(ctx, JobStateFailed, []uint64{event1.ID})
	require.Equal(t, errDeadAckedAsFailed, err)
	require.Empty(t, ackFailedNoAttemptsLeft)

	acknowledgedAttempt3, err := queue.Acknowledge(ctx, JobStateDead, []uint64{event1.ID})
	require.NoError(t, err)
	require.Equal(t, []uint64{event1.ID}, acknowledgedAttempt3, "one event must be acknowledged")

	dequeuedAttempt4, err := queue.Dequeue(ctx, "praefect", "storage-1", 100500)
	require.NoError(t, err)
	require.Empty(t, dequeuedAttempt4, "all attempts to process job were used")

	dequeuedAttempt5, err := queue.Dequeue(ctx, "praefect", "storage-2", 100500)
	require.NoError(t, err)
	require.Len(t, dequeuedAttempt5, 1, "only single event must be fetched for this storage")

	expAttempt5 := ReplicationEvent{
		ID:        2,
		State:     JobStateInProgress,
		Attempt:   2,
		LockID:    "praefect|storage-2|/project/path-1",
		CreatedAt: event2.CreatedAt,              // it is a hack to have same time for both
		UpdatedAt: dequeuedAttempt5[0].UpdatedAt, // it is a hack to have same time for both
		Job:       job2,
	}
	require.Equal(t, expAttempt5, dequeuedAttempt5[0])

	acknowledgedAttempt5, err := queue.Acknowledge(ctx, JobStateDead, []uint64{event2.ID})
	require.NoError(t, err)
	require.Equal(t, []uint64{event2.ID}, acknowledgedAttempt5, "one event must be acknowledged")

	dequeuedAttempt6, err := queue.Dequeue(ctx, "praefect", "storage-2", 100500)
	require.NoError(t, err)
	require.Empty(t, dequeuedAttempt6, "all jobs marked as completed for this storage")
}

func TestMemoryReplicationEventQueue_ConcurrentAccess(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	queue := NewMemoryReplicationEventQueue(config.Config{})

	job1 := ReplicationJob{
		Change:            UpdateRepo,
		RelativePath:      "/project/path-1",
		VirtualStorage:    "praefect",
		TargetNodeStorage: "storage-1",
		SourceNodeStorage: "storage-0",
	}

	job2 := ReplicationJob{
		Change:            UpdateRepo,
		RelativePath:      "/project/path-1",
		VirtualStorage:    "praefect",
		TargetNodeStorage: "storage-2",
		SourceNodeStorage: "storage-0",
	}

	eventType1 := ReplicationEvent{Job: job1}
	eventType2 := ReplicationEvent{Job: job2}

	var checkScenario = func(wg *sync.WaitGroup, event ReplicationEvent, state JobState) {
		defer wg.Done()

		created, err := queue.Enqueue(ctx, event)
		require.NoError(t, err)

		dequeued, err := queue.Dequeue(ctx, "praefect", created.Job.TargetNodeStorage, 100500)
		require.NoError(t, err)
		require.Len(t, dequeued, 1)
		require.Equal(t, created.Job, dequeued[0].Job)

		ackIDs, err := queue.Acknowledge(ctx, state, []uint64{created.ID})
		require.NoError(t, err)
		require.Len(t, ackIDs, 1)
		require.Equal(t, created.ID, ackIDs[0])

		nothing, err := queue.Dequeue(ctx, "praefect", created.Job.TargetNodeStorage, 100500)
		require.NoError(t, err)
		require.Len(t, nothing, 0)
	}

	wg := &sync.WaitGroup{}
	wg.Add(2)
	go checkScenario(wg, eventType1, JobStateCompleted)
	go checkScenario(wg, eventType2, JobStateCancelled)
	wg.Wait()
}
