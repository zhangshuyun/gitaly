package praefect

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"google.golang.org/grpc/metadata"

	"github.com/sirupsen/logrus"
)

// Replicator performs the actual replication logic between two nodes
type Replicator interface {
	Replicate(ctx context.Context, source Repository, target Node) error
}

type defaultReplicator struct {
	log *logrus.Logger
}

func (dr defaultReplicator) Replicate(ctx context.Context, source Repository, target Node) error {
	repository := &gitalypb.Repository{
		StorageName:  target.Storage,
		RelativePath: source.RelativePath,
	}
	remoteRepository := &gitalypb.Repository{
		StorageName:  source.Storage,
		RelativePath: source.RelativePath,
	}

	repositoryClient := gitalypb.NewRepositoryServiceClient(target.cc)
	remoteClient := gitalypb.NewRemoteServiceClient(target.cc)

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
	log         *logrus.Logger
	dataStore   Datastore
	coordinator *Coordinator
	storage     string     // which replica is this replicator responsible for?
	replicator  Replicator // does the actual replication logic

	// whitelist contains the project names of the repos we wish to replicate
	whitelist map[string]struct{}
}

// ReplMgrOpt allows a replicator to be configured with additional options
type ReplMgrOpt func(*ReplMgr)

// NewReplMgr initializes a replication manager with the provided dependencies
// and options
func NewReplMgr(storage string, log *logrus.Logger, ds Datastore, c *Coordinator, opts ...ReplMgrOpt) ReplMgr {
	r := ReplMgr{
		log:         log,
		dataStore:   ds,
		whitelist:   map[string]struct{}{},
		replicator:  defaultReplicator{log},
		storage:     storage,
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
func (r ReplMgr) ScheduleReplication(ctx context.Context, repo Repository) error {
	_, ok := r.whitelist[repo.RelativePath]
	if !ok {
		r.log.WithField(logKeyProjectPath, repo.RelativePath).
			Infof("project %q is not whitelisted for replication", repo.RelativePath)
		return nil
	}

	id, err := r.dataStore.CreateSecondaryReplJobs(repo)
	if err != nil {
		return err
	}

	r.log.Infof(
		"replication manager for storage %q created replication job with ID %d",
		r.storage,
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
		jobs, err := r.dataStore.GetJobs(JobStatePending|JobStateReady, r.storage, 10)
		if err != nil {
			return err
		}

		if len(jobs) == 0 {
			r.log.Tracef("no jobs for %s, checking again in %s", r.storage, jobFetchInterval)

			select {
			// TODO: exponential backoff when no queries are returned
			case <-time.After(jobFetchInterval):
				continue

			case <-ctx.Done():
				return ctx.Err()
			}
		}

		r.log.Debugf("fetched replication jobs: %#v", jobs)

		for _, job := range jobs {
			r.log.WithField(logWithReplJobID, job.ID).
				Infof("processing replication job %#v", job)
			node, err := r.coordinator.GetStorageNode(job.Target)
			if err != nil {
				return err
			}

			if err := r.dataStore.UpdateReplJob(job.ID, JobStateInProgress); err != nil {
				return err
			}

			gitalyServer, err := r.dataStore.GetPrimary()
			if err != nil {
				return err
			}

			gitalyServers := storage.GitalyServers{
				gitalyServer.Name: {
					"address": gitalyServer.ListenAddr,
					"token":   gitalyServer.Token,
				},
			}

			gitalyServersJSON, err := json.Marshal(gitalyServers)
			if err != nil {
				return err
			}

			ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("gitaly-servers", base64.StdEncoding.EncodeToString(gitalyServersJSON)))

			if err := r.replicator.Replicate(ctx, job.Source, node); err != nil {
				return err
			}

			r.log.WithField(logWithReplJobID, job.ID).
				Info("completed replication")
			if err := r.dataStore.UpdateReplJob(job.ID, JobStateComplete); err != nil {
				return err
			}
		}
	}
}
