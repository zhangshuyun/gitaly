package datastore

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func ContractTestCountDeadReplicationJobs(t *testing.T, q ReplicationEventQueue) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	// take the time here to include the also the
	// completed and cancelled jobs in timerange
	beforeOldest := time.Now()

	const target = "target"
	ackJobsToDeath := func(t *testing.T) {
		t.Helper()

		for {
			jobs, err := q.Dequeue(ctx, target, 1)
			require.NoError(t, err)
			if len(jobs) == 0 {
				break
			}

			for _, job := range jobs {
				state := JobStateFailed
				if job.Attempt == 0 {
					state = JobStateDead
				}

				_, err := q.Acknowledge(ctx, state, []uint64{job.ID})
				require.NoError(t, err)
			}
		}
	}

	// postgres only handles timestamps with a microsecond resolution thus
	// we have to work with the time in microsecond sized steps
	const tick = time.Microsecond

	// add some other job states to the datastore to ensure they are not counted
	for relPath, state := range map[string]JobState{"repo/completed-job": JobStateCompleted, "repo/cancelled-job": JobStateCancelled} {
		_, err := q.Enqueue(ctx, ReplicationEvent{Job: ReplicationJob{RelativePath: relPath, TargetNodeStorage: target}})
		require.NoError(t, err)

		jobs, err := q.Dequeue(ctx, target, 1)
		require.NoError(t, err)

		_, err = q.Acknowledge(ctx, state, []uint64{jobs[0].ID})
		require.NoError(t, err)
	}

	oldest, err := q.Enqueue(ctx, ReplicationEvent{Job: ReplicationJob{RelativePath: "old", TargetNodeStorage: target}})
	require.NoError(t, err)

	afterOldest := oldest.CreatedAt.Add(tick)

	dead, err := q.CountDeadReplicationJobs(ctx, beforeOldest, afterOldest)
	require.NoError(t, err)
	require.Empty(t, dead, "should not include ready jobs")

	jobs, err := q.Dequeue(ctx, target, 1)
	require.NoError(t, err)

	_, err = q.Acknowledge(ctx, JobStateFailed, []uint64{jobs[0].ID})
	require.NoError(t, err)

	dead, err = q.CountDeadReplicationJobs(ctx, beforeOldest, afterOldest)
	require.NoError(t, err)
	require.Empty(t, dead, "should not include failed jobs")

	ackJobsToDeath(t)
	dead, err = q.CountDeadReplicationJobs(ctx, beforeOldest, afterOldest)
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"old": 1}, dead, "should include dead job")

	middle, err := q.Enqueue(ctx, ReplicationEvent{Job: ReplicationJob{RelativePath: "new", TargetNodeStorage: target}})
	require.NoError(t, err)

	ackJobsToDeath(t)
	dead, err = q.CountDeadReplicationJobs(ctx, beforeOldest, middle.CreatedAt.Add(tick))
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"old": 1, "new": 1}, dead, "should include both dead jobs")

	newest, err := q.Enqueue(ctx, ReplicationEvent{Job: ReplicationJob{RelativePath: "new", TargetNodeStorage: target}})
	require.NoError(t, err)

	ackJobsToDeath(t)
	dead, err = q.CountDeadReplicationJobs(ctx, beforeOldest, newest.CreatedAt.Add(tick))
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"old": 1, "new": 2}, dead, "dead job are grouped by relative path")

	dead, err = q.CountDeadReplicationJobs(ctx, middle.CreatedAt, newest.CreatedAt.Add(-tick))
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"new": 1}, dead, "should only count the in-between dead job")
}

func TestMemoryCountDeadReplicationJobs(t *testing.T) {
	ContractTestCountDeadReplicationJobs(t, NewMemoryReplicationEventQueue())
}

func TestMemoryCountDeadReplicationJobsLimit(t *testing.T) {
	q := NewMemoryReplicationEventQueue().(*memoryReplicationEventQueue)
	q.maxDeadJobs = 2

	ctx, cancel := testhelper.Context()
	defer cancel()

	const target = "target"

	beforeAll := time.Now()
	for i := 0; i < q.maxDeadJobs+1; i++ {
		job, err := q.Enqueue(ctx, ReplicationEvent{Job: ReplicationJob{RelativePath: fmt.Sprintf("job-%d", i), TargetNodeStorage: target}})
		require.NoError(t, err)

		for i := 0; i < job.Attempt; i++ {
			_, err := q.Dequeue(ctx, target, 1)
			require.NoError(t, err)

			state := JobStateFailed
			if i == job.Attempt-1 {
				state = JobStateDead
			}

			_, err = q.Acknowledge(ctx, state, []uint64{job.ID})
			require.NoError(t, err)
		}
	}

	dead, err := q.CountDeadReplicationJobs(ctx, beforeAll, time.Now())
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"job-1": 1, "job-2": 1}, dead, "should only include the last two dead jobs")
}

func TestMemoryReplicationEventQueue(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	queue := NewMemoryReplicationEventQueue()

	noEvents, err := queue.Dequeue(ctx, "storage-1", 100500)
	require.NoError(t, err)
	require.Empty(t, noEvents, "no events as queue is empty")

	noAcknowledged, err := queue.Acknowledge(ctx, JobStateCompleted, []uint64{1, 2, 3})
	require.NoError(t, err)
	require.Empty(t, noAcknowledged, "no acknowledged ids as queue is empty")

	job1 := ReplicationJob{
		Change:            UpdateRepo,
		RelativePath:      "/project/path-1",
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
		LockID:    "storage-1|/project/path-1",
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
		LockID:    "storage-2|/project/path-1",
		CreatedAt: event2.CreatedAt, // it is a hack to have same time for both
		Job:       job2,
	}
	require.Equal(t, expEvent2, event2)

	dequeuedAttempt1, err := queue.Dequeue(ctx, "storage-1", 100500)
	require.NoError(t, err)
	require.Len(t, dequeuedAttempt1, 1, "only single event must be fetched for this storage")

	expAttempt1 := ReplicationEvent{
		ID:        1,
		State:     JobStateInProgress,
		Attempt:   2,
		LockID:    "storage-1|/project/path-1",
		CreatedAt: event1.CreatedAt,              // it is a hack to have same time for both
		UpdatedAt: dequeuedAttempt1[0].UpdatedAt, // it is a hack to have same time for both
		Job:       job1,
	}
	require.Equal(t, expAttempt1, dequeuedAttempt1[0])

	acknowledgedAttempt1, err := queue.Acknowledge(ctx, JobStateFailed, []uint64{event1.ID, event2.ID})
	require.NoError(t, err)
	require.Equal(t, []uint64{event1.ID}, acknowledgedAttempt1, "one event must be acknowledged")

	dequeuedAttempt2, err := queue.Dequeue(ctx, "storage-1", 100500)
	require.NoError(t, err)
	require.Len(t, dequeuedAttempt2, 1, "only single event must be fetched for this storage")

	expAttempt2 := ReplicationEvent{
		ID:        1,
		State:     JobStateInProgress,
		Attempt:   1,
		LockID:    "storage-1|/project/path-1",
		CreatedAt: event1.CreatedAt,              // it is a hack to have same time for both
		UpdatedAt: dequeuedAttempt2[0].UpdatedAt, // it is a hack to have same time for both
		Job:       job1,
	}
	require.Equal(t, expAttempt2, dequeuedAttempt2[0])

	acknowledgedAttempt2, err := queue.Acknowledge(ctx, JobStateFailed, []uint64{event1.ID})
	require.NoError(t, err)
	require.Equal(t, []uint64{event1.ID}, acknowledgedAttempt2, "one event must be acknowledged")

	dequeuedAttempt3, err := queue.Dequeue(ctx, "storage-1", 100500)
	require.NoError(t, err)
	require.Len(t, dequeuedAttempt3, 1, "only single event must be fetched for this storage")

	expAttempt3 := ReplicationEvent{
		ID:        1,
		State:     JobStateInProgress,
		Attempt:   0,
		LockID:    "storage-1|/project/path-1",
		CreatedAt: event1.CreatedAt,              // it is a hack to have same time for both
		UpdatedAt: dequeuedAttempt3[0].UpdatedAt, // it is a hack to have same time for both
		Job:       job1,
	}
	require.Equal(t, expAttempt3, dequeuedAttempt3[0])

	ackFailedNoAttemptsLeft, err := queue.Acknowledge(ctx, JobStateFailed, []uint64{event1.ID})
	require.Error(t, errDeadAckedAsFailed, err)
	require.Empty(t, ackFailedNoAttemptsLeft)

	acknowledgedAttempt3, err := queue.Acknowledge(ctx, JobStateDead, []uint64{event1.ID})
	require.NoError(t, err)
	require.Equal(t, []uint64{event1.ID}, acknowledgedAttempt3, "one event must be acknowledged")

	dequeuedAttempt4, err := queue.Dequeue(ctx, "storage-1", 100500)
	require.NoError(t, err)
	require.Empty(t, dequeuedAttempt4, "all attempts to process job were used")

	dequeuedAttempt5, err := queue.Dequeue(ctx, "storage-2", 100500)
	require.NoError(t, err)
	require.Len(t, dequeuedAttempt5, 1, "only single event must be fetched for this storage")

	expAttempt5 := ReplicationEvent{
		ID:        2,
		State:     JobStateInProgress,
		Attempt:   2,
		LockID:    "storage-2|/project/path-1",
		CreatedAt: event2.CreatedAt,              // it is a hack to have same time for both
		UpdatedAt: dequeuedAttempt5[0].UpdatedAt, // it is a hack to have same time for both
		Job:       job2,
	}
	require.Equal(t, expAttempt5, dequeuedAttempt5[0])

	acknowledgedAttempt5, err := queue.Acknowledge(ctx, JobStateDead, []uint64{event2.ID})
	require.NoError(t, err)
	require.Equal(t, []uint64{event2.ID}, acknowledgedAttempt5, "one event must be acknowledged")

	dequeuedAttempt6, err := queue.Dequeue(ctx, "storage-2", 100500)
	require.NoError(t, err)
	require.Empty(t, dequeuedAttempt6, "all jobs marked as completed for this storage")
}

func TestMemoryReplicationEventQueue_ConcurrentAccess(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	queue := NewMemoryReplicationEventQueue()

	job1 := ReplicationJob{
		Change:            UpdateRepo,
		RelativePath:      "/project/path-1",
		TargetNodeStorage: "storage-1",
		SourceNodeStorage: "storage-0",
	}

	job2 := ReplicationJob{
		Change:            UpdateRepo,
		RelativePath:      "/project/path-1",
		TargetNodeStorage: "storage-2",
		SourceNodeStorage: "storage-0",
	}

	eventType1 := ReplicationEvent{Job: job1}
	eventType2 := ReplicationEvent{Job: job2}

	var checkScenario = func(wg *sync.WaitGroup, event ReplicationEvent, state JobState) {
		defer wg.Done()

		created, err := queue.Enqueue(ctx, event)
		require.NoError(t, err)

		dequeued, err := queue.Dequeue(ctx, created.Job.TargetNodeStorage, 100500)
		require.NoError(t, err)
		require.Len(t, dequeued, 1)
		require.Equal(t, created.Job, dequeued[0].Job)

		ackIDs, err := queue.Acknowledge(ctx, state, []uint64{created.ID})
		require.NoError(t, err)
		require.Len(t, ackIDs, 1)
		require.Equal(t, created.ID, ackIDs[0])

		nothing, err := queue.Dequeue(ctx, created.Job.TargetNodeStorage, 100500)
		require.NoError(t, err)
		require.Len(t, nothing, 0)
	}

	wg := &sync.WaitGroup{}
	wg.Add(2)
	go checkScenario(wg, eventType1, JobStateCompleted)
	go checkScenario(wg, eventType2, JobStateCancelled)
	wg.Wait()
}
