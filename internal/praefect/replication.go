package praefect

import (
	"context"
)

// ReplMan is a replication manager for handling replication jobs
type ReplMan struct {
	l Logger
	// whitelist contains the project names of the repos we wish to replicate
	whitelist map[string]struct{}
}

type ReplManOpt func(*ReplMan)

func NewReplMan(opts ...ReplManOpt) *ReplMan {
	m := &ReplMan{
		whitelist: map[string]struct{}{},
	}

	for _, opt := range opts {
		opt(m)
	}

	return m
}

// WithWhitelist will configure a whitelist for repos to allow replication
func WithWhitelist(whitelistedRepos []string) ReplManOpt {
	return func(m *ReplMan) {
		for _, r := range whitelistedRepos {
			m.whitelist[r] = struct{}{}
		}
	}
}

// ReplCoordinator represents all the coordinator functionality the replication
// manager relies on
type ReplCoordinator interface {
	// ReplicationQueue returns a stream of jobs from
	ReplicationQueue(context.Context) (<-chan ReplJob, error)

	// CompleteJob reports if a job was completed. A non-nil jobErr indicates
	// the job was not successful.
	CompleteJob(ctx context.Context, ID string, jobErr error) error
}

func (rm *ReplMan) ProcessJobs(ctx context.Context, rc ReplCoordinator) error {
	jobQ, err := rc.ReplicationQueue(ctx)

	for {
		var (
			job ReplJob
			ok  bool
		)

		select {

		// context cancelled
		case <-ctx.Done():
			return ctx.Err()

		case job, ok = <-jobQ:
			if !ok { // channel closed
				return nil
			}

		}

		jobErr := job.Replica.PullReplication(ctx, job.Primary)

		err = rc.CompleteJob(ctx, job.ID, jobErr)
		if err != nil {
			rm.l.Errorf("unable to report replication job completion for %+v", job.ID)
		}
	}
}
