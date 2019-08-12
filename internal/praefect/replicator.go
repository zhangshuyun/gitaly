package praefect

import (
	"context"
	"fmt"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"

	"github.com/sirupsen/logrus"
)

// Replicator performs the actual replication logic between two nodes
type Replicator interface {
	Replicate(ctx context.Context, job ReplJob, target *grpc.ClientConn) error
}

type defaultReplicator struct {
	log *logrus.Entry
}

func (dr defaultReplicator) Replicate(ctx context.Context, job ReplJob, targetCC *grpc.ClientConn) error {
	repository := &gitalypb.Repository{
		StorageName:  job.TargetNode.Storage,
		RelativePath: job.Repository.RelativePath,
	}

	remoteRepository := &gitalypb.Repository{
		StorageName:  job.SourceNode.Storage,
		RelativePath: job.Repository.RelativePath,
	}

	repositoryClient := gitalypb.NewRepositoryServiceClient(targetCC)
	remoteClient := gitalypb.NewRemoteServiceClient(targetCC)

	// CreateRepository is idempotent
	if _, err := repositoryClient.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{
		Repository: repository,
	}); err != nil {
		return fmt.Errorf("failed to create repository: %v", err)
	}

	if _, err := remoteClient.FetchInternalRemote(ctx, &gitalypb.FetchInternalRemoteRequest{
		Repository:       repository,
		RemoteRepository: remoteRepository,
	}); err != nil {
		return err
	}
	// TODO: ensure attribute files are synced
	// https://gitlab.com/gitlab-org/gitaly/issues/1655

	// TODO: ensure objects/info/alternates are synced
	// https://gitlab.com/gitlab-org/gitaly/issues/1674

	return nil
}

// ReplMgr is a replication manager for handling replication jobs
type ReplMgr struct {
	log         *logrus.Entry
	datastore   Datastore
	coordinator *Coordinator
	targetNode  string     // which replica is this replicator responsible for?
	replicator  Replicator // does the actual replication logic

	// whitelist contains the project names of the repos we wish to replicate
	whitelist map[string]struct{}
}

// ReplMgrOpt allows a replicator to be configured with additional options
type ReplMgrOpt func(*ReplMgr)

// NewReplMgr initializes a replication manager with the provided dependencies
// and options
func NewReplMgr(targetNode string, log *logrus.Entry, datastore Datastore, c *Coordinator, opts ...ReplMgrOpt) ReplMgr {
	r := ReplMgr{
		log:         log,
		datastore:   datastore,
		whitelist:   map[string]struct{}{},
		replicator:  defaultReplicator{log},
		targetNode:  targetNode,
		coordinator: c,
	}

	for _, opt := range opts {
		opt(&r)
	}

	return r
}

// WithWhitelist will configure a whitelist for repos to allow replication
func WithWhitelist(whitelistedRepos []string) ReplMgrOpt {
	return func(r *ReplMgr) {
		for _, repo := range whitelistedRepos {
			r.whitelist[repo] = struct{}{}
		}
	}
}

// WithReplicator overrides the default replicator
func WithReplicator(r Replicator) ReplMgrOpt {
	return func(rm *ReplMgr) {
		rm.replicator = r
	}
}

// ScheduleReplication will store a replication job in the datastore for later
// execution. It filters out projects that are not whitelisted.
// TODO: add a parameter to delay replication
func (r ReplMgr) ScheduleReplication(ctx context.Context, repo models.Repository) error {
	_, ok := r.whitelist[repo.RelativePath]
	if !ok {
		r.log.WithField(logKeyProjectPath, repo.RelativePath).
			Infof("project %q is not whitelisted for replication", repo.RelativePath)
		return nil
	}

	id, err := r.datastore.CreateReplicaReplJobs(repo.RelativePath)
	if err != nil {
		return err
	}

	r.log.Infof(
		"replication manager for targetNode %q created replication job with ID %d",
		r.targetNode,
		id,
	)

	return nil
}

const (
	jobFetchInterval = 10 * time.Millisecond
	logWithReplJobID = "replication-job-ID"
)

// ProcessBacklog will process queued jobs. It will block while processing jobs.
func (r ReplMgr) ProcessBacklog(ctx context.Context) error {
	for {
		nodes, err := r.datastore.GetStorageNodes()
		if err != nil {
			return nil
		}

		for _, node := range nodes {
			jobs, err := r.datastore.GetJobs(JobStateReady, node.ID, 10)
			if err != nil {
				return err
			}

			if len(jobs) == 0 {
				r.log.Tracef("no jobs for %d, checking again in %s", node.ID, jobFetchInterval)

				select {
				// TODO: exponential backoff when no queries are returned
				case <-time.After(jobFetchInterval):
					continue

				case <-ctx.Done():
					return ctx.Err()
				}
			}

			for _, job := range jobs {
				r.log.WithField(logWithReplJobID, job.ID).
					Infof("processing replication job %#v", job)

				if err := r.datastore.UpdateReplJob(job.ID, JobStateInProgress); err != nil {
					return err
				}

				ctx, err = helper.InjectGitalyServers(ctx, job.Repository.Primary.Storage, job.SourceNode.Address, "")
				if err != nil {
					return err
				}

				cc, err := r.coordinator.GetConnection(job.TargetNode.ID)
				if err != nil {
					return err
				}

				if err := r.replicator.Replicate(ctx, job, cc); err != nil {
					r.log.WithField(logWithReplJobID, job.ID).WithError(err).Error("error when replicating")
					return err
				}

				if err := r.datastore.UpdateReplJob(job.ID, JobStateComplete); err != nil {
					return err
				}
			}
		}
	}
}
