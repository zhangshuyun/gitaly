// +build postgres

package datastore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestPostgresReplicationEventQueue_CountDeadReplicationJobs(t *testing.T) {
	ContractTestCountDeadReplicationJobs(t, PostgresReplicationEventQueue{getDB(t).DB})
}

func TestPostgresReplicationEventQueue_Enqueue(t *testing.T) {
	db := getDB(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	queue := PostgresReplicationEventQueue{db.DB}

	eventType := ReplicationEvent{
		Job: ReplicationJob{
			Change:            UpdateRepo,
			RelativePath:      "/project/path-1",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect",
			Params:            nil,
		},
	}

	actualEvent, err := queue.Enqueue(ctx, eventType) // initial event
	require.NoError(t, err)
	actualEvent.CreatedAt = time.Time{} // we need to setup it to default because it is not possible to get it beforehand for expected

	expLock := LockRow{ID: "praefect|gitaly-1|/project/path-1", Acquired: false}

	expEvent := ReplicationEvent{
		ID:      1,
		State:   JobStateReady,
		Attempt: 3,
		LockID:  "praefect|gitaly-1|/project/path-1",
		Job: ReplicationJob{
			Change:            UpdateRepo,
			RelativePath:      "/project/path-1",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect",
			Params:            nil,
		},
	}

	require.Equal(t, expEvent, actualEvent)
	requireEvents(t, ctx, db, []ReplicationEvent{expEvent})
	requireLocks(t, ctx, db, []LockRow{expLock}) // expected a new lock for new event
	db.RequireRowsInTable(t, "replication_queue_job_lock", 0)
}

func TestPostgresReplicationEventQueue_EnqueueMultiple(t *testing.T) {
	db := getDB(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	queue := PostgresReplicationEventQueue{db.DB}

	eventType1 := ReplicationEvent{
		Job: ReplicationJob{
			Change:            UpdateRepo,
			RelativePath:      "/project/path-1",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect-0",
			Params:            nil,
		},
	}

	eventType2 := ReplicationEvent{
		Job: ReplicationJob{
			Change:            RenameRepo,
			RelativePath:      "/project/path-1",
			TargetNodeStorage: "gitaly-2",
			SourceNodeStorage: "",
			VirtualStorage:    "praefect-0",
			Params:            Params{"RelativePath": "/project/path-1-renamed"},
		},
	}

	eventType3 := ReplicationEvent{
		Job: ReplicationJob{
			Change:            UpdateRepo,
			RelativePath:      "/project/path-2",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect-1",
			Params:            nil,
		},
	}

	event1, err := queue.Enqueue(ctx, eventType1) // initial event
	require.NoError(t, err)

	expLock1 := LockRow{ID: "praefect-0|gitaly-1|/project/path-1", Acquired: false}
	expLock2 := LockRow{ID: "praefect-0|gitaly-2|/project/path-1", Acquired: false}
	expLock3 := LockRow{ID: "praefect-1|gitaly-1|/project/path-2", Acquired: false}

	expEvent1 := ReplicationEvent{
		ID:      event1.ID,
		State:   "ready",
		Attempt: 3,
		LockID:  "praefect-0|gitaly-1|/project/path-1",
		Job: ReplicationJob{
			Change:            UpdateRepo,
			RelativePath:      "/project/path-1",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect-0",
			Params:            nil,
		},
	}

	requireEvents(t, ctx, db, []ReplicationEvent{expEvent1})
	requireLocks(t, ctx, db, []LockRow{expLock1}) // expected a new lock for new event
	db.RequireRowsInTable(t, "replication_queue_job_lock", 0)

	event2, err := queue.Enqueue(ctx, eventType1) // repeat of the same event
	require.NoError(t, err)

	expEvent2 := ReplicationEvent{
		ID:      event2.ID,
		State:   "ready",
		Attempt: 3,
		LockID:  "praefect-0|gitaly-1|/project/path-1",
		Job: ReplicationJob{
			Change:            UpdateRepo,
			RelativePath:      "/project/path-1",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect-0",
			Params:            nil,
		},
	}

	requireEvents(t, ctx, db, []ReplicationEvent{expEvent1, expEvent2})
	requireLocks(t, ctx, db, []LockRow{expLock1}) // expected still one the same lock for repeated event

	event3, err := queue.Enqueue(ctx, eventType2) // event for another target
	require.NoError(t, err)

	expEvent3 := ReplicationEvent{
		ID:      event3.ID,
		State:   JobStateReady,
		Attempt: 3,
		LockID:  "praefect-0|gitaly-2|/project/path-1",
		Job: ReplicationJob{
			Change:            RenameRepo,
			RelativePath:      "/project/path-1",
			TargetNodeStorage: "gitaly-2",
			SourceNodeStorage: "",
			VirtualStorage:    "praefect-0",
			Params:            Params{"RelativePath": "/project/path-1-renamed"},
		},
	}

	requireEvents(t, ctx, db, []ReplicationEvent{expEvent1, expEvent2, expEvent3})
	requireLocks(t, ctx, db, []LockRow{expLock1, expLock2}) // the new lock for another target repeated event

	event4, err := queue.Enqueue(ctx, eventType3) // event for another repo
	require.NoError(t, err)

	expEvent4 := ReplicationEvent{
		ID:      event4.ID,
		State:   JobStateReady,
		Attempt: 3,
		LockID:  "praefect-1|gitaly-1|/project/path-2",
		Job: ReplicationJob{
			Change:            UpdateRepo,
			RelativePath:      "/project/path-2",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect-1",
			Params:            nil,
		},
	}

	requireEvents(t, ctx, db, []ReplicationEvent{expEvent1, expEvent2, expEvent3, expEvent4})
	requireLocks(t, ctx, db, []LockRow{expLock1, expLock2, expLock3}) // the new lock for same target but for another repo

	db.RequireRowsInTable(t, "replication_queue_job_lock", 0) // there is no fetches it must be empty
}

func TestPostgresReplicationEventQueue_Dequeue(t *testing.T) {
	db := getDB(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	queue := PostgresReplicationEventQueue{db.DB}

	event := ReplicationEvent{
		Job: ReplicationJob{
			Change:            UpdateRepo,
			RelativePath:      "/project/path-1",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect",
			Params:            nil,
		},
	}

	event, err := queue.Enqueue(ctx, event)
	require.NoError(t, err, "failed to fill in event queue")

	noEvents, err := queue.Dequeue(ctx, "praefect", "not existing storage", 5)
	require.NoError(t, err)
	require.Len(t, noEvents, 0, "there must be no events dequeued for not existing storage")

	expectedEvent := event
	expectedEvent.State = JobStateInProgress
	expectedEvent.Attempt = 2

	expectedLock := LockRow{ID: event.LockID, Acquired: true} // as we deque events we acquire lock for processing

	expectedJobLock := JobLockRow{JobID: event.ID, LockID: event.LockID} // and there is a track if job is under processing in separate table

	actual, err := queue.Dequeue(ctx, event.Job.VirtualStorage, event.Job.TargetNodeStorage, 5)
	require.NoError(t, err)

	for i := range actual {
		actual[i].UpdatedAt = nil // it is not possible to determine update_at value as it is generated on UPDATE in database
	}
	require.Equal(t, []ReplicationEvent{expectedEvent}, actual)

	// there is only one single lock for all fetched events
	requireLocks(t, ctx, db, []LockRow{expectedLock})
	requireJobLocks(t, ctx, db, []JobLockRow{expectedJobLock})
}

// expected results are listed as literals on purpose to be more explicit about what is going on with data
func TestPostgresReplicationEventQueue_DequeueMultiple(t *testing.T) {
	db := getDB(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	queue := PostgresReplicationEventQueue{db.DB}

	eventType1 := ReplicationEvent{
		Job: ReplicationJob{
			Change:            UpdateRepo,
			RelativePath:      "/project/path-1",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect",
			Params:            nil,
		},
	}

	eventType2 := ReplicationEvent{
		Job: ReplicationJob{
			Change:            DeleteRepo,
			RelativePath:      "/project/path-1",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "",
			VirtualStorage:    "praefect",
			Params:            nil,
		},
	}

	eventType3 := ReplicationEvent{
		Job: ReplicationJob{
			Change:            RenameRepo,
			RelativePath:      "/project/path-2",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect",
			Params:            Params{"RelativePath": "/project/path-2-renamed"},
		},
	}

	events := []ReplicationEvent{eventType1, eventType1, eventType2, eventType1, eventType3} // events to fill in the queue
	for i := range events {
		var err error
		events[i], err = queue.Enqueue(ctx, events[i])
		require.NoError(t, err, "failed to fill in event queue")
	}

	// first request to deque
	const limitFirstN = 3 // limit on the amount of jobs we are gonna to deque

	expectedEvents1 := make([]ReplicationEvent, limitFirstN)
	expectedJobLocks1 := make([]JobLockRow, limitFirstN)
	for i := range expectedEvents1 {
		expectedEvents1[i] = events[i]
		expectedEvents1[i].State = JobStateInProgress
		expectedEvents1[i].Attempt = 2

		expectedJobLocks1[i].JobID = expectedEvents1[i].ID
		expectedJobLocks1[i].LockID = "praefect|gitaly-1|/project/path-1"
	}

	// we expect only first two types of events by limiting count to 3
	dequeuedEvents1, err := queue.Dequeue(ctx, "praefect", "gitaly-1", limitFirstN)
	require.NoError(t, err)
	for i := range dequeuedEvents1 {
		dequeuedEvents1[i].UpdatedAt = nil // it is not possible to determine update_at value as it is generated on UPDATE in database
	}
	require.Equal(t, expectedEvents1, dequeuedEvents1)

	requireLocks(t, ctx, db, []LockRow{
		// there is only one single lock for all fetched events because of their 'repo' and 'target' combination
		{ID: "praefect|gitaly-1|/project/path-1", Acquired: true},
		{ID: "praefect|gitaly-1|/project/path-2", Acquired: false},
	})
	requireJobLocks(t, ctx, db, expectedJobLocks1)

	// second request to deque

	// there must be only last event fetched from the queue
	expectedEvents2 := []ReplicationEvent{events[len(events)-1]}
	expectedEvents2[0].State = JobStateInProgress
	expectedEvents2[0].Attempt = 2

	expectedJobLocks2 := []JobLockRow{{JobID: 5, LockID: "praefect|gitaly-1|/project/path-2"}}

	dequeuedEvents2, err := queue.Dequeue(ctx, "praefect", "gitaly-1", 100500)
	require.NoError(t, err)
	require.Len(t, dequeuedEvents2, 1, "only one event must be fetched from the queue")

	dequeuedEvents2[0].UpdatedAt = nil // it is not possible to determine update_at value as it is generated on UPDATE in database
	require.Equal(t, expectedEvents2, dequeuedEvents2)

	requireLocks(t, ctx, db, []LockRow{
		{ID: "praefect|gitaly-1|/project/path-1", Acquired: true},
		{ID: "praefect|gitaly-1|/project/path-2", Acquired: true},
	})
	requireJobLocks(t, ctx, db, append(expectedJobLocks1, expectedJobLocks2...))

	// this event wasn't not consumed by the first deque because of the limit
	// it is also wasn't consumed by the second  deque because there is already a lock acquired for this type of event
	remainingEvents := []ReplicationEvent{events[3]}
	expectedEvents := append(append(expectedEvents1, remainingEvents...), expectedEvents2...)
	requireEvents(t, ctx, db, expectedEvents)
}

func TestPostgresReplicationEventQueue_Acknowledge(t *testing.T) {
	db := getDB(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	queue := PostgresReplicationEventQueue{db.DB}

	event := ReplicationEvent{
		Job: ReplicationJob{
			Change:            UpdateRepo,
			RelativePath:      "/project/path-1",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect",
			Params:            nil,
		},
	}

	event, err := queue.Enqueue(ctx, event)
	require.NoError(t, err, "failed to fill in event queue")

	actual, err := queue.Dequeue(ctx, event.Job.VirtualStorage, event.Job.TargetNodeStorage, 100)
	require.NoError(t, err)

	// as we deque events we acquire lock for processing
	requireLocks(t, ctx, db, []LockRow{{ID: event.LockID, Acquired: true}})
	requireJobLocks(t, ctx, db, []JobLockRow{{JobID: event.ID, LockID: event.LockID}})

	acknowledged, err := queue.Acknowledge(ctx, JobStateCompleted, []uint64{actual[0].ID, 100500})
	require.NoError(t, err)
	require.Equal(t, []uint64{actual[0].ID}, acknowledged)

	event.State = JobStateCompleted
	event.Attempt = 2
	requireEvents(t, ctx, db, []ReplicationEvent{event})
	// lock must be released as the event was acknowledged and there are no other events left protected under this lock
	requireLocks(t, ctx, db, []LockRow{{ID: event.LockID, Acquired: false}})
	// all associated with acknowledged event tracking bindings between lock and event must be removed
	requireJobLocks(t, ctx, db, nil)
}

func TestPostgresReplicationEventQueue_AcknowledgeMultiple(t *testing.T) {
	db := getDB(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	queue := PostgresReplicationEventQueue{db.DB}

	eventType1 := ReplicationEvent{
		Job: ReplicationJob{
			Change:            UpdateRepo,
			RelativePath:      "/project/path-1",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect",
			Params:            nil,
		},
	}

	eventType2 := ReplicationEvent{
		Job: ReplicationJob{
			Change:            DeleteRepo,
			RelativePath:      "/project/path-2",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "",
			VirtualStorage:    "praefect",
			Params:            nil,
		},
	}

	eventType3 := ReplicationEvent{
		Job: ReplicationJob{
			Change:            UpdateRepo,
			RelativePath:      "/project/path-3",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect",
			Params:            nil,
		},
	}

	eventType4 := ReplicationEvent{
		Job: ReplicationJob{
			Change:            UpdateRepo,
			RelativePath:      "/project/path-1",
			TargetNodeStorage: "gitaly-2",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "praefect",
			Params:            nil,
		},
	}

	events := []ReplicationEvent{eventType1, eventType1, eventType2, eventType1, eventType3, eventType2, eventType4} // events to fill in the queue
	for i := range events {
		var err error
		events[i], err = queue.Enqueue(ctx, events[i])
		require.NoError(t, err, "failed to fill in event queue")
	}

	// we expect only first two types of events by limiting count to 3
	dequeuedEvents1, err := queue.Dequeue(ctx, "praefect", "gitaly-1", 3)
	require.NoError(t, err)
	require.Len(t, dequeuedEvents1, 3)
	requireLocks(t, ctx, db, []LockRow{
		{ID: "praefect|gitaly-1|/project/path-1", Acquired: true},
		{ID: "praefect|gitaly-1|/project/path-2", Acquired: true},
		{ID: "praefect|gitaly-1|/project/path-3", Acquired: false},
		{ID: "praefect|gitaly-2|/project/path-1", Acquired: false},
	})
	requireJobLocks(t, ctx, db, []JobLockRow{
		{JobID: 1, LockID: "praefect|gitaly-1|/project/path-1"},
		{JobID: 2, LockID: "praefect|gitaly-1|/project/path-1"},
		{JobID: 3, LockID: "praefect|gitaly-1|/project/path-2"},
	})

	// release lock for events of second type
	acknowledge1, err := queue.Acknowledge(ctx, JobStateFailed, []uint64{3})
	require.NoError(t, err)
	require.Equal(t, []uint64{3}, acknowledge1)
	requireLocks(t, ctx, db, []LockRow{
		{ID: "praefect|gitaly-1|/project/path-1", Acquired: true},
		{ID: "praefect|gitaly-1|/project/path-2", Acquired: false},
		{ID: "praefect|gitaly-1|/project/path-3", Acquired: false},
		{ID: "praefect|gitaly-2|/project/path-1", Acquired: false},
	})
	requireJobLocks(t, ctx, db, []JobLockRow{
		{JobID: 1, LockID: "praefect|gitaly-1|/project/path-1"},
		{JobID: 2, LockID: "praefect|gitaly-1|/project/path-1"},
	})

	dequeuedEvents2, err := queue.Dequeue(ctx, "praefect", "gitaly-1", 3)
	require.NoError(t, err)
	require.Len(t, dequeuedEvents2, 2, "expected: event of type 3 and of type 2 ('failed' will  be fetched for retry)")
	requireLocks(t, ctx, db, []LockRow{
		{ID: "praefect|gitaly-1|/project/path-1", Acquired: true},
		{ID: "praefect|gitaly-1|/project/path-2", Acquired: true},
		{ID: "praefect|gitaly-1|/project/path-3", Acquired: true},
		{ID: "praefect|gitaly-2|/project/path-1", Acquired: false},
	})
	requireJobLocks(t, ctx, db, []JobLockRow{
		{JobID: 1, LockID: "praefect|gitaly-1|/project/path-1"},
		{JobID: 2, LockID: "praefect|gitaly-1|/project/path-1"},
		{JobID: 5, LockID: "praefect|gitaly-1|/project/path-3"},
		{JobID: 3, LockID: "praefect|gitaly-1|/project/path-2"},
	})

	acknowledge2, err := queue.Acknowledge(ctx, JobStateCompleted, []uint64{1, 3})
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 3}, acknowledge2)
	requireLocks(t, ctx, db, []LockRow{
		{ID: "praefect|gitaly-1|/project/path-1", Acquired: true},
		{ID: "praefect|gitaly-1|/project/path-2", Acquired: false},
		{ID: "praefect|gitaly-1|/project/path-3", Acquired: true},
		{ID: "praefect|gitaly-2|/project/path-1", Acquired: false},
	})
	requireJobLocks(t, ctx, db, []JobLockRow{
		{JobID: 2, LockID: "praefect|gitaly-1|/project/path-1"},
		{JobID: 5, LockID: "praefect|gitaly-1|/project/path-3"},
	})

	dequeuedEvents3, err := queue.Dequeue(ctx, "praefect", "gitaly-2", 3)
	require.NoError(t, err)
	require.Len(t, dequeuedEvents3, 1, "expected: event of type 4")
	requireLocks(t, ctx, db, []LockRow{
		{ID: "praefect|gitaly-1|/project/path-1", Acquired: true},
		{ID: "praefect|gitaly-1|/project/path-2", Acquired: false},
		{ID: "praefect|gitaly-1|/project/path-3", Acquired: true},
		{ID: "praefect|gitaly-2|/project/path-1", Acquired: true},
	})
	requireJobLocks(t, ctx, db, []JobLockRow{
		{JobID: 2, LockID: "praefect|gitaly-1|/project/path-1"},
		{JobID: 5, LockID: "praefect|gitaly-1|/project/path-3"},
		{JobID: 7, LockID: "praefect|gitaly-2|/project/path-1"},
	})

	acknowledged3, err := queue.Acknowledge(ctx, JobStateCompleted, []uint64{2, 5, 7})
	require.NoError(t, err)
	require.Equal(t, []uint64{2, 5, 7}, acknowledged3)
	requireLocks(t, ctx, db, []LockRow{
		{ID: "praefect|gitaly-1|/project/path-1", Acquired: false},
		{ID: "praefect|gitaly-1|/project/path-2", Acquired: false},
		{ID: "praefect|gitaly-1|/project/path-3", Acquired: false},
		{ID: "praefect|gitaly-2|/project/path-1", Acquired: false},
	})
	requireJobLocks(t, ctx, db, nil)

	dequeuedEvents4, err := queue.Dequeue(ctx, "praefect", "gitaly-1", 100500)
	require.NoError(t, err)
	require.Len(t, dequeuedEvents4, 2, "expected: event of type 4")
	requireLocks(t, ctx, db, []LockRow{
		{ID: "praefect|gitaly-1|/project/path-1", Acquired: true},
		{ID: "praefect|gitaly-1|/project/path-2", Acquired: true},
		{ID: "praefect|gitaly-1|/project/path-3", Acquired: false},
		{ID: "praefect|gitaly-2|/project/path-1", Acquired: false},
	})
	requireJobLocks(t, ctx, db, []JobLockRow{
		{JobID: 4, LockID: "praefect|gitaly-1|/project/path-1"},
		{JobID: 6, LockID: "praefect|gitaly-1|/project/path-2"},
	})

	newEvent, err := queue.Enqueue(ctx, eventType1)
	require.NoError(t, err)

	acknowledge4, err := queue.Acknowledge(ctx, JobStateCompleted, []uint64{newEvent.ID})
	require.NoError(t, err)
	require.Equal(t, ([]uint64)(nil), acknowledge4) // event that was not dequeued can't be acknowledged
	var newEventState string
	require.NoError(t, db.QueryRow("SELECT state FROM replication_queue WHERE id = $1", newEvent.ID).Scan(&newEventState))
	require.Equal(t, "ready", newEventState, "no way to acknowledge event that is not in in_progress state(was not dequeued)")
	requireLocks(t, ctx, db, []LockRow{
		{ID: "praefect|gitaly-1|/project/path-1", Acquired: true},
		{ID: "praefect|gitaly-1|/project/path-2", Acquired: true},
		{ID: "praefect|gitaly-1|/project/path-3", Acquired: false},
		{ID: "praefect|gitaly-2|/project/path-1", Acquired: false},
	})
	requireJobLocks(t, ctx, db, []JobLockRow{
		{JobID: 4, LockID: "praefect|gitaly-1|/project/path-1"},
		{JobID: 6, LockID: "praefect|gitaly-1|/project/path-2"},
	})
}

func TestPostgresReplicationEventQueue_GetOutdatedRepositories(t *testing.T) {
	const (
		virtualStorage = "test-virtual-storage"
		oldPrimary     = "old-primary"
		newPrimary     = "new-primary"
		secondary      = "secondary"
	)

	now := time.Now()
	offset := func(d time.Duration) *time.Time {
		t := now.Add(d)
		return &t
	}

	for _, tc := range []struct {
		desc     string
		events   []ReplicationEvent
		error    error
		expected map[string][]string
	}{
		{
			desc: "basic scenarios work",
			events: []ReplicationEvent{
				{
					State: JobStateReady,
					Job: ReplicationJob{
						VirtualStorage:    "wrong-virtual-storage",
						SourceNodeStorage: oldPrimary,
						TargetNodeStorage: newPrimary,
						RelativePath:      "repo-1",
					},
				},
				{
					State: JobStateDead,
					Job: ReplicationJob{
						VirtualStorage:    virtualStorage,
						SourceNodeStorage: "wrong-source-node",
						TargetNodeStorage: newPrimary,
						RelativePath:      "repo-1",
					},
				},
				{
					State: JobStateCompleted,
					Job: ReplicationJob{
						VirtualStorage:    virtualStorage,
						SourceNodeStorage: oldPrimary,
						TargetNodeStorage: newPrimary,
						RelativePath:      "completed-job-ignored",
					},
				},
				{
					State: JobStateDead,
					Job: ReplicationJob{
						VirtualStorage:    virtualStorage,
						SourceNodeStorage: oldPrimary,
						TargetNodeStorage: newPrimary,
						RelativePath:      "repo-1",
					},
				},
				{
					State: JobStateInProgress,
					Job: ReplicationJob{
						VirtualStorage:    virtualStorage,
						SourceNodeStorage: oldPrimary,
						TargetNodeStorage: newPrimary,
						RelativePath:      "repo-2",
					},
				},
				{
					State: JobStateFailed,
					Job: ReplicationJob{
						VirtualStorage:    virtualStorage,
						SourceNodeStorage: oldPrimary,
						TargetNodeStorage: secondary,
						RelativePath:      "repo-2",
					},
				},
			},
			expected: map[string][]string{
				"repo-1": {newPrimary},
				"repo-2": {newPrimary, secondary},
			},
		},
		{
			desc: "search considers null updated_at as latest",
			events: []ReplicationEvent{
				{
					State:     JobStateCompleted,
					UpdatedAt: offset(0),
					Job: ReplicationJob{
						VirtualStorage:    virtualStorage,
						SourceNodeStorage: oldPrimary,
						TargetNodeStorage: newPrimary,
						RelativePath:      "repo-1",
					},
				},
				{
					State: JobStateReady,
					Job: ReplicationJob{
						VirtualStorage:    virtualStorage,
						SourceNodeStorage: oldPrimary,
						TargetNodeStorage: newPrimary,
						RelativePath:      "repo-1",
					},
				},
			},
			expected: map[string][]string{
				"repo-1": []string{newPrimary},
			},
		},
		{
			// Search should terminate at the reference storage and not consider
			// earlier nodes.
			desc: "search terminates terminates at the reference storage",
			events: []ReplicationEvent{
				{
					State:     JobStateDead,
					UpdatedAt: offset(0),
					Job: ReplicationJob{
						VirtualStorage:    virtualStorage,
						SourceNodeStorage: "even-older-primary",
						TargetNodeStorage: oldPrimary,
						RelativePath:      "repo-1",
					},
				},
				{
					State:     JobStateDead,
					UpdatedAt: offset(time.Second),
					Job: ReplicationJob{
						VirtualStorage:    virtualStorage,
						SourceNodeStorage: oldPrimary,
						TargetNodeStorage: newPrimary,
						RelativePath:      "repo-1",
					},
				},
				{
					State:     JobStateCompleted,
					UpdatedAt: offset(2 * time.Second),
					Job: ReplicationJob{
						VirtualStorage:    virtualStorage,
						SourceNodeStorage: oldPrimary,
						TargetNodeStorage: newPrimary,
						RelativePath:      "repo-1",
					},
				},
			},
			expected: map[string][]string{},
		},
		{
			// Incomplete jobs from other nodes than the previous writable
			// primary do not indicate inconsistent state and should be
			// ignored.
			desc: "up to date with failed jobs from secondaries",
			events: []ReplicationEvent{
				{
					State:     JobStateCompleted,
					UpdatedAt: offset(0),
					Job: ReplicationJob{
						VirtualStorage:    virtualStorage,
						SourceNodeStorage: oldPrimary,
						TargetNodeStorage: newPrimary,
						RelativePath:      "repo-1",
					},
				},
				{
					State:     JobStateDead,
					UpdatedAt: offset(time.Second),
					Job: ReplicationJob{
						VirtualStorage:    virtualStorage,
						SourceNodeStorage: secondary,
						TargetNodeStorage: newPrimary,
						RelativePath:      "repo-1",
					},
				},
			},
			expected: map[string][]string{},
		},
		{
			// Node that experienced data loss on failover but was later
			// reconciled from the previous writable primary should
			// contain complete data.
			desc: "up to date with earlier failed job from old primary",
			events: []ReplicationEvent{
				{
					State:     JobStateDead,
					UpdatedAt: offset(0),
					Job: ReplicationJob{
						VirtualStorage:    virtualStorage,
						SourceNodeStorage: oldPrimary,
						TargetNodeStorage: newPrimary,
						RelativePath:      "repo-1",
					},
				},
				{
					State:     JobStateCompleted,
					UpdatedAt: offset(time.Second),
					Job: ReplicationJob{
						VirtualStorage:    virtualStorage,
						SourceNodeStorage: oldPrimary,
						TargetNodeStorage: newPrimary,
						RelativePath:      "repo-1",
					},
				},
			},
			expected: map[string][]string{},
		},
		{
			// If the node was reconciled from an outdated secondary,
			// it will contain the same outdated data as the secondary.
			// The node should be considered outdated as well.
			desc: "reconciled from outdated secondary",
			events: []ReplicationEvent{
				{
					State:     JobStateDead,
					UpdatedAt: offset(0),
					Job: ReplicationJob{
						VirtualStorage:    virtualStorage,
						SourceNodeStorage: oldPrimary,
						TargetNodeStorage: newPrimary,
						RelativePath:      "repo-1",
					},
				},
				{
					State:     JobStateDead,
					UpdatedAt: offset(time.Second),
					Job: ReplicationJob{
						VirtualStorage:    virtualStorage,
						SourceNodeStorage: oldPrimary,
						TargetNodeStorage: secondary,
						RelativePath:      "repo-1",
					},
				},
				{
					State:     JobStateCompleted,
					UpdatedAt: offset(2 * time.Second),
					Job: ReplicationJob{
						VirtualStorage:    virtualStorage,
						SourceNodeStorage: secondary,
						TargetNodeStorage: newPrimary,
						RelativePath:      "repo-1",
					},
				},
			},
			expected: map[string][]string{
				"repo-1": {newPrimary, secondary},
			},
		},
		{
			// If the node is reconciled from an up to date secondary,
			// the node contains the latest data as well.
			desc: "reconciled from up to date secondary",
			events: []ReplicationEvent{
				{
					State:     JobStateDead,
					UpdatedAt: offset(0),
					Job: ReplicationJob{
						VirtualStorage:    virtualStorage,
						SourceNodeStorage: oldPrimary,
						TargetNodeStorage: newPrimary,
						RelativePath:      "repo-1",
					},
				},
				{
					State:     JobStateCompleted,
					UpdatedAt: offset(time.Second),
					Job: ReplicationJob{
						VirtualStorage:    virtualStorage,
						SourceNodeStorage: oldPrimary,
						TargetNodeStorage: secondary,
						RelativePath:      "repo-1",
					},
				},
				{
					State:     JobStateCompleted,
					UpdatedAt: offset(2 * time.Second),
					Job: ReplicationJob{
						VirtualStorage:    virtualStorage,
						SourceNodeStorage: secondary,
						TargetNodeStorage: newPrimary,
						RelativePath:      "repo-1",
					},
				},
			},
			expected: map[string][]string{},
		},
		{
			// Reconciling two nodes back and forth causes a cycle. We should report the
			// repositories as out of date as there is no way to construct a path back to
			// the previous writable primary. This prevents us from verifying whether the
			// nodes are up to date with the previous writable primary.
			//
			// Reconciling towards the previous writable primary breaks the precondition for
			// the search that the previous writable primary contains the full data and has
			// not received writes after failing over. Results from such state are not accurate.
			desc: "two way cycle reports out of date",
			events: []ReplicationEvent{
				{
					State:     JobStateCompleted,
					UpdatedAt: offset(0),
					Job: ReplicationJob{
						VirtualStorage:    virtualStorage,
						SourceNodeStorage: secondary,
						TargetNodeStorage: newPrimary,
						RelativePath:      "repo-1",
					},
				},
				{
					State:     JobStateCompleted,
					UpdatedAt: offset(time.Second),
					Job: ReplicationJob{
						VirtualStorage:    virtualStorage,
						SourceNodeStorage: newPrimary,
						TargetNodeStorage: secondary,
						RelativePath:      "repo-1",
					},
				},
			},
			expected: map[string][]string{
				"repo-1": []string{newPrimary, secondary},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			db := getDB(t)

			for _, event := range tc.events {
				db.MustExec(t, "INSERT INTO replication_queue (state, updated_at, job) VALUES ($1, $2, $3)",
					event.State, event.UpdatedAt, event.Job,
				)
			}

			actual, err := NewPostgresReplicationEventQueue(db).GetOutdatedRepositories(ctx, virtualStorage, oldPrimary)
			require.NoError(t, err)
			require.Equal(t, tc.expected, actual)
		})
	}
}

func TestPostgresReplicationEventQueue_GetUpToDateStorages(t *testing.T) {
	db := getDB(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	source := PostgresReplicationEventQueue{qc: db}

	t.Run("single 'ready' job for single storage", func(t *testing.T) {
		db.TruncateAll(t)

		db.MustExec(t, `
			INSERT INTO replication_queue
				(job, updated_at, state)
			VALUES
				('{"virtual_storage": "vs1", "target_node_storage": "s1", "relative_path": "path-1"}', '2020-01-01 00:00:00', 'ready')`,
		)

		ss, err := source.GetUpToDateStorages(ctx, "vs1", "path-1")
		require.NoError(t, err)
		require.ElementsMatch(t, []string{}, ss)
	})

	t.Run("single 'dead' job for single storage", func(t *testing.T) {
		db.TruncateAll(t)

		db.MustExec(t, `
			INSERT INTO replication_queue
				(job, updated_at, state)
			VALUES
				('{"virtual_storage": "vs1", "target_node_storage": "s1", "relative_path": "path-1"}', '2020-01-01 00:00:00', 'dead')`,
		)

		ss, err := source.GetUpToDateStorages(ctx, "vs1", "path-1")
		require.NoError(t, err)
		require.ElementsMatch(t, []string{}, ss)
	})

	t.Run("single 'failed' job for single storage", func(t *testing.T) {
		db.TruncateAll(t)

		db.MustExec(t, `
			INSERT INTO replication_queue
				(job, updated_at, state)
			VALUES
				('{"virtual_storage": "vs1", "target_node_storage": "s1", "relative_path": "path-1"}', '2020-01-01 00:00:00', 'failed')`,
		)

		ss, err := source.GetUpToDateStorages(ctx, "vs1", "path-1")
		require.NoError(t, err)
		require.ElementsMatch(t, []string{}, ss)
	})

	t.Run("single 'completed' job for single storage", func(t *testing.T) {
		db.TruncateAll(t)

		db.MustExec(t, `
			INSERT INTO replication_queue
				(job, updated_at, state)
			VALUES
				('{"virtual_storage": "vs1", "target_node_storage": "s1", "relative_path": "path-1"}', '2020-01-01 00:00:00', 'completed')`,
		)

		ss, err := source.GetUpToDateStorages(ctx, "vs1", "path-1")
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"s1"}, ss)
	})

	t.Run("multiple 'completed' jobs for single storage but different repos", func(t *testing.T) {
		db.TruncateAll(t)

		db.MustExec(t, `
			INSERT INTO replication_queue
				(job, updated_at, state)
			VALUES
				('{"virtual_storage": "vs1", "target_node_storage": "s1", "relative_path": "path-1"}', '2020-01-01 00:00:00', 'completed'),
				('{"virtual_storage": "vs1", "target_node_storage": "s2", "relative_path": "path-2"}', '2020-01-01 00:00:00', 'completed')`,
		)

		ss, err := source.GetUpToDateStorages(ctx, "vs1", "path-1")
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"s1"}, ss)
	})

	t.Run("last jobs are 'completed' for multiple storages", func(t *testing.T) {
		db.TruncateAll(t)

		db.MustExec(t, `
			INSERT INTO replication_queue
				(job, updated_at, state)
			VALUES
				('{"virtual_storage": "vs1", "target_node_storage": "s1", "relative_path": "path-1"}', '2020-01-01 00:00:00', 'completed'),
				('{"virtual_storage": "vs1", "target_node_storage": "s2", "relative_path": "path-1"}', '2020-01-01 00:00:00', 'completed')`,
		)

		ss, err := source.GetUpToDateStorages(ctx, "vs1", "path-1")
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"s1", "s2"}, ss)
	})

	t.Run("last jobs are 'completed' for multiple storages but different virtuals", func(t *testing.T) {
		db.TruncateAll(t)

		db.MustExec(t, `
			INSERT INTO replication_queue
				(job, updated_at, state)
			VALUES
				('{"virtual_storage": "vs1", "target_node_storage": "s1", "relative_path": "path-1"}', '2020-01-01 00:00:00', 'completed'),
				('{"virtual_storage": "vs2", "target_node_storage": "s1", "relative_path": "path-1"}', '2020-01-01 00:00:00', 'completed')`,
		)

		ss, err := source.GetUpToDateStorages(ctx, "vs1", "path-1")
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"s1"}, ss)
	})

	t.Run("lasts are in 'completed' and 'in_progress' for different storages", func(t *testing.T) {
		db.TruncateAll(t)

		db.MustExec(t, `
			INSERT INTO replication_queue
				(job, updated_at, state)
			VALUES
				('{"virtual_storage": "vs1", "target_node_storage": "s1", "relative_path": "path-1"}', '2020-01-01 00:00:00', 'completed'),
				('{"virtual_storage": "vs1", "target_node_storage": "s2", "relative_path": "path-1"}', '2020-01-01 00:00:00', 'in_progress')`,
		)

		ss, err := source.GetUpToDateStorages(ctx, "vs1", "path-1")
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"s1"}, ss)
	})

	t.Run("lasts are in 'dead', 'ready', 'failed' and 'in_progress' for different storages", func(t *testing.T) {
		db.TruncateAll(t)

		db.MustExec(t, `
			INSERT INTO replication_queue
				(job, updated_at, state)
			VALUES
				('{"virtual_storage": "vs1", "target_node_storage": "s1", "relative_path": "path-1"}', '2020-01-01 00:00:00', 'dead'),
				('{"virtual_storage": "vs1", "target_node_storage": "s2", "relative_path": "path-1"}', '2020-01-01 00:00:00', 'ready'),
				('{"virtual_storage": "vs1", "target_node_storage": "s3", "relative_path": "path-1"}', '2020-01-01 00:00:00', 'failed'),
				('{"virtual_storage": "vs1", "target_node_storage": "s4", "relative_path": "path-1"}', '2020-01-01 00:00:00', 'in_progress')`,
		)

		ss, err := source.GetUpToDateStorages(ctx, "vs1", "path-1")
		require.NoError(t, err)
		require.ElementsMatch(t, []string{}, ss)
	})

	t.Run("last is not 'completed'", func(t *testing.T) {
		db.TruncateAll(t)

		db.MustExec(t, `
			INSERT INTO replication_queue
				(job, updated_at, state)
			VALUES
				('{"virtual_storage": "vs1", "target_node_storage": "s1", "relative_path": "path-1"}', '2020-01-01 00:00:01', 'dead'),
				('{"virtual_storage": "vs1", "target_node_storage": "s1", "relative_path": "path-1"}', '2020-01-01 00:00:00', 'completed'),

				('{"virtual_storage": "vs1", "target_node_storage": "s2", "relative_path": "path-1"}', '2020-01-01 00:00:01', 'ready'),
				('{"virtual_storage": "vs1", "target_node_storage": "s2", "relative_path": "path-1"}', '2020-01-01 00:00:00', 'completed'),

				('{"virtual_storage": "vs1", "target_node_storage": "s3", "relative_path": "path-1"}', '2020-01-01 00:00:01', 'failed'),
				('{"virtual_storage": "vs1", "target_node_storage": "s3", "relative_path": "path-1"}', '2020-01-01 00:00:00', 'completed'),

				('{"virtual_storage": "vs1", "target_node_storage": "s4", "relative_path": "path-1"}', '2020-01-01 00:00:01', 'failed'),
				('{"virtual_storage": "vs1", "target_node_storage": "s4", "relative_path": "path-1"}', '2020-01-01 00:00:00', 'completed')`,
		)

		ss, err := source.GetUpToDateStorages(ctx, "vs1", "path-1")
		require.NoError(t, err)
		require.ElementsMatch(t, []string{}, ss)
	})

	t.Run("multiple virtuals with multiple storages", func(t *testing.T) {
		db.TruncateAll(t)

		db.MustExec(t, `
			INSERT INTO replication_queue
				(job, updated_at, state)
			VALUES
				('{"virtual_storage": "vs1", "target_node_storage": "s1", "relative_path": "path-1"}', '2020-01-01 00:00:01', 'dead'),
				('{"virtual_storage": "vs1", "target_node_storage": "s1", "relative_path": "path-1"}', '2020-01-01 00:00:00', 'completed'),

				('{"virtual_storage": "vs1", "target_node_storage": "s2", "relative_path": "path-1"}', '2020-01-01 00:00:01', 'completed'),
				('{"virtual_storage": "vs1", "target_node_storage": "s2", "relative_path": "path-1"}', '2020-01-01 00:00:00', 'dead'),

				('{"virtual_storage": "vs2", "target_node_storage": "s3", "relative_path": "path-1"}', '2020-01-01 00:00:01', 'completed'),
				('{"virtual_storage": "vs2", "target_node_storage": "s3", "relative_path": "path-1"}', '2020-01-01 00:00:00', 'completed'),

				('{"virtual_storage": "vs1", "target_node_storage": "s4", "relative_path": "path-2"}', '2020-01-01 00:00:01', 'failed'),
				('{"virtual_storage": "vs1", "target_node_storage": "s4", "relative_path": "path-2"}', '2020-01-01 00:00:00', 'completed'),

				('{"virtual_storage": "vs1", "target_node_storage": "s5", "relative_path": "path-2"}', '2020-01-01 00:00:00', 'completed')`,
		)

		ss, err := source.GetUpToDateStorages(ctx, "vs1", "path-1")
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"s2"}, ss)
	})
}

func requireEvents(t *testing.T, ctx context.Context, db glsql.DB, expected []ReplicationEvent) {
	t.Helper()

	// as it is not possible to expect exact time of entity creation/update we do not fetch it from database
	// and we do not take it into account from expected values.
	exp := make([]ReplicationEvent, len(expected)) // make a copy to avoid side effects for passed values
	for i, e := range expected {
		exp[i] = e
		// set to default values as they would not be initialized from database
		exp[i].CreatedAt = time.Time{}
		exp[i].UpdatedAt = nil
	}

	sqlStmt := `SELECT id, state, attempt, lock_id, job FROM replication_queue ORDER BY id`
	rows, err := db.QueryContext(ctx, sqlStmt)
	require.NoError(t, err)

	actual, err := scanReplicationEvents(rows)
	require.NoError(t, err)
	require.Equal(t, exp, actual)
}

// LockRow exists only for testing purposes and represents entries from replication_queue_lock table.
type LockRow struct {
	ID       string
	Acquired bool
}

func requireLocks(t *testing.T, ctx context.Context, db glsql.DB, expected []LockRow) {
	t.Helper()

	sqlStmt := `SELECT id, acquired FROM replication_queue_lock`
	rows, err := db.QueryContext(ctx, sqlStmt)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close(), "completion of result fetching") }()

	var actual []LockRow
	for rows.Next() {
		var entry LockRow
		require.NoError(t, rows.Scan(&entry.ID, &entry.Acquired), "failed to scan entry")
		actual = append(actual, entry)
	}
	require.NoError(t, rows.Err(), "completion of result loop scan")
	require.ElementsMatch(t, expected, actual)
}

// JobLockRow exists only for testing purposes and represents entries from replication_queue_job_lock table.
type JobLockRow struct {
	JobID       uint64
	LockID      string
	TriggeredAt time.Time
}

func requireJobLocks(t *testing.T, ctx context.Context, db glsql.DB, expected []JobLockRow) {
	t.Helper()

	sqlStmt := `SELECT job_id, lock_id FROM replication_queue_job_lock ORDER BY triggered_at`
	rows, err := db.QueryContext(ctx, sqlStmt)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close(), "completion of result fetching") }()

	var actual []JobLockRow
	for rows.Next() {
		var entry JobLockRow
		require.NoError(t, rows.Scan(&entry.JobID, &entry.LockID), "failed to scan entry")
		actual = append(actual, entry)
	}
	require.NoError(t, rows.Err(), "completion of result loop scan")
	require.ElementsMatch(t, expected, actual)
}
