package praefect

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metrics"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// Replicator performs the actual replication logic between two nodes
type Replicator interface {
	// Replicate propagates changes from the source to the target
	Replicate(ctx context.Context, job datastore.ReplJob, source, target *grpc.ClientConn) error
	// Destroy will remove the target repo on the specified target connection
	Destroy(ctx context.Context, job datastore.ReplJob, target *grpc.ClientConn) error
	// Rename will rename(move) the target repo on the specified target connection
	Rename(ctx context.Context, job datastore.ReplJob, target *grpc.ClientConn) error
}

type defaultReplicator struct {
	log *logrus.Entry
}

func (dr defaultReplicator) Replicate(ctx context.Context, job datastore.ReplJob, sourceCC, targetCC *grpc.ClientConn) error {
	targetRepository := &gitalypb.Repository{
		StorageName:  job.TargetNode.Storage,
		RelativePath: job.RelativePath,
	}

	sourceRepository := &gitalypb.Repository{
		StorageName:  job.SourceNode.Storage,
		RelativePath: job.RelativePath,
	}

	targetRepositoryClient := gitalypb.NewRepositoryServiceClient(targetCC)

	if _, err := targetRepositoryClient.ReplicateRepository(ctx, &gitalypb.ReplicateRepositoryRequest{
		Source:     sourceRepository,
		Repository: targetRepository,
	}); err != nil {
		return fmt.Errorf("failed to replicate repository: %v", err)
	}

	// check if the repository has an object pool
	sourceObjectPoolClient := gitalypb.NewObjectPoolServiceClient(sourceCC)

	resp, err := sourceObjectPoolClient.GetObjectPool(ctx, &gitalypb.GetObjectPoolRequest{
		Repository: sourceRepository,
	})
	if err != nil {
		return err
	}

	sourceObjectPool := resp.GetObjectPool()

	if sourceObjectPool != nil {
		targetObjectPoolClient := gitalypb.NewObjectPoolServiceClient(targetCC)
		targetObjectPool := *sourceObjectPool
		targetObjectPool.GetRepository().StorageName = targetRepository.GetStorageName()
		if _, err := targetObjectPoolClient.LinkRepositoryToObjectPool(ctx, &gitalypb.LinkRepositoryToObjectPoolRequest{
			ObjectPool: &targetObjectPool,
			Repository: targetRepository,
		}); err != nil {
			return err
		}
	}

	checksumsMatch, err := dr.confirmChecksums(ctx, gitalypb.NewRepositoryServiceClient(sourceCC), targetRepositoryClient, sourceRepository, targetRepository)
	if err != nil {
		return err
	}

	// TODO: Do something meaninful with the result of confirmChecksums if checksums do not match
	if !checksumsMatch {
		dr.log.WithFields(logrus.Fields{
			"primary": sourceRepository,
			"replica": targetRepository,
		}).Error("checksums do not match")
	}

	// TODO: ensure attribute files are synced
	// https://gitlab.com/gitlab-org/gitaly/issues/1655

	// TODO: ensure objects/info/alternates are synced
	// https://gitlab.com/gitlab-org/gitaly/issues/1674

	return nil
}

func (dr defaultReplicator) Destroy(ctx context.Context, job datastore.ReplJob, targetCC *grpc.ClientConn) error {
	targetRepo := &gitalypb.Repository{
		StorageName:  job.TargetNode.Storage,
		RelativePath: job.RelativePath,
	}

	repoSvcClient := gitalypb.NewRepositoryServiceClient(targetCC)

	_, err := repoSvcClient.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{
		Repository: targetRepo,
	})

	return err
}

func (dr defaultReplicator) Rename(ctx context.Context, job datastore.ReplJob, targetCC *grpc.ClientConn) error {
	targetRepo := &gitalypb.Repository{
		StorageName:  job.TargetNode.Storage,
		RelativePath: job.RelativePath,
	}

	repoSvcClient := gitalypb.NewRepositoryServiceClient(targetCC)

	val, found := job.Params["RelativePath"]
	if !found {
		return errors.New("no 'RelativePath' parameter for rename")
	}

	relativePath, ok := val.(string)
	if !ok {
		return fmt.Errorf("parameter 'RelativePath' has unexpected type: %T", relativePath)
	}

	_, err := repoSvcClient.RenameRepository(ctx, &gitalypb.RenameRepositoryRequest{
		Repository:   targetRepo,
		RelativePath: relativePath,
	})

	return err
}

func getChecksumFunc(ctx context.Context, client gitalypb.RepositoryServiceClient, repo *gitalypb.Repository, checksum *string) func() error {
	return func() error {
		primaryChecksumRes, err := client.CalculateChecksum(ctx, &gitalypb.CalculateChecksumRequest{
			Repository: repo,
		})
		if err != nil {
			return err
		}
		*checksum = primaryChecksumRes.GetChecksum()
		return nil
	}
}

func (dr defaultReplicator) confirmChecksums(ctx context.Context, primaryClient, replicaClient gitalypb.RepositoryServiceClient, primary, replica *gitalypb.Repository) (bool, error) {
	g, gCtx := errgroup.WithContext(ctx)

	var primaryChecksum, replicaChecksum string

	g.Go(getChecksumFunc(gCtx, primaryClient, primary, &primaryChecksum))
	g.Go(getChecksumFunc(gCtx, replicaClient, replica, &replicaChecksum))

	if err := g.Wait(); err != nil {
		return false, err
	}

	dr.log.WithFields(logrus.Fields{
		"primary":          primary,
		"replica":          replica,
		"primary_checksum": primaryChecksum,
		"replica_checksum": replicaChecksum,
	}).Info("replication finished")

	return primaryChecksum == replicaChecksum, nil
}

// ReplMgr is a replication manager for handling replication jobs
type ReplMgr struct {
	log               *logrus.Entry
	datastore         datastore.Datastore
	nodeManager       nodes.Manager
	virtualStorage    string     // which replica is this replicator responsible for?
	replicator        Replicator // does the actual replication logic
	replQueueMetric   metrics.Gauge
	replLatencyMetric metrics.Histogram
	replJobTimeout    time.Duration
	// whitelist contains the project names of the repos we wish to replicate
	whitelist map[string]struct{}
}

// ReplMgrOpt allows a replicator to be configured with additional options
type ReplMgrOpt func(*ReplMgr)

// WithQueueMetric is an option to set the queue size prometheus metric
func WithQueueMetric(g metrics.Gauge) func(*ReplMgr) {
	return func(m *ReplMgr) {
		m.replQueueMetric = g
	}
}

// WithLatencyMetric is an option to set the queue size prometheus metric
func WithLatencyMetric(h metrics.Histogram) func(*ReplMgr) {
	return func(m *ReplMgr) {
		m.replLatencyMetric = h
	}
}

// NewReplMgr initializes a replication manager with the provided dependencies
// and options
func NewReplMgr(virtualStorage string, log *logrus.Entry, datastore datastore.Datastore, nodeMgr nodes.Manager, opts ...ReplMgrOpt) ReplMgr {
	r := ReplMgr{
		log:               log,
		datastore:         datastore,
		whitelist:         map[string]struct{}{},
		replicator:        defaultReplicator{log},
		virtualStorage:    virtualStorage,
		nodeManager:       nodeMgr,
		replLatencyMetric: prometheus.NewHistogram(prometheus.HistogramOpts{}),
		replQueueMetric:   prometheus.NewGauge(prometheus.GaugeOpts{}),
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

const (
	logWithReplJobID  = "replication_job_id"
	logWithReplSource = "replication_job_source"
	logWithReplTarget = "replication_job_target"
)

type backoff func() time.Duration
type backoffReset func()

// BackoffFunc is a function that n turn provides a pair of functions backoff and backoffReset
type BackoffFunc func() (backoff, backoffReset)

// ExpBackoffFunc generates a backoffFunc based off of start and max time durations
func ExpBackoffFunc(start time.Duration, max time.Duration) BackoffFunc {
	return func() (backoff, backoffReset) {
		const factor = 2
		duration := start

		return func() time.Duration {
				defer func() {
					duration *= time.Duration(factor)
					if (duration) >= max {
						duration = max
					}
				}()
				return duration
			}, func() {
				duration = start
			}
	}
}

const (
	maxAttempts = 3
)

func (r ReplMgr) getPrimaryAndSecondaries() (primary nodes.Node, secondaries []nodes.Node, err error) {
	shard, err := r.nodeManager.GetShard(r.virtualStorage)
	if err != nil {
		return nil, nil, err
	}

	primary, err = shard.GetPrimary()
	if err != nil {
		return nil, nil, err
	}

	secondaries, err = shard.GetSecondaries()
	if err != nil {
		return nil, nil, err
	}

	return primary, secondaries, nil
}

// ProcessBacklog will process queued jobs. It will block while processing jobs.
func (r ReplMgr) ProcessBacklog(ctx context.Context, b BackoffFunc) error {
	backoff, reset := b()

	for {
		var totalJobs int
		primary, secondaries, err := r.getPrimaryAndSecondaries()
		if err == nil {
			for _, secondary := range secondaries {
				jobs, err := r.datastore.GetJobs(datastore.JobStateReady|datastore.JobStateFailed, secondary.GetStorage(), 10)
				if err != nil {
					return err
				}

				totalJobs += len(jobs)

				type replicatedKey struct {
					change                   datastore.ChangeType
					repoPath, source, target string
				}
				reposReplicated := make(map[replicatedKey]struct{})

				for _, job := range jobs {
					if job.Attempts >= maxAttempts {
						if err := r.datastore.UpdateReplJobState(job.ID, datastore.JobStateDead); err != nil {
							r.log.WithError(err).Error("error when updating replication job status to cancelled")
						}
						continue
					}

					var replicationKey replicatedKey
					switch job.Change {
					// this optimization could be done only for Update and Delete replication jobs as we treat them as idempotent
					// Update - there is no much profit from executing multiple fetches for the same target from the same source one by one
					// Delete - there is no way how we could remove already removed repository
					// that is why those Jobs needs to be tracked and marked as Cancelled (removed from queue without execution).
					case datastore.UpdateRepo, datastore.DeleteRepo:
						replicationKey = replicatedKey{
							change:   job.Change,
							repoPath: job.RelativePath,
							source:   job.SourceNode.Storage,
							target:   job.TargetNode.Storage,
						}

						if _, ok := reposReplicated[replicationKey]; ok {
							if err := r.datastore.UpdateReplJobState(job.ID, datastore.JobStateCancelled); err != nil {
								r.log.WithError(err).Error("error when updating replication job status to cancelled")
							}
							continue
						}
					}

					if err = r.processReplJob(ctx, job, primary.GetConnection(), secondary.GetConnection()); err != nil {
						r.log.WithFields(logrus.Fields{
							logWithReplJobID: job.ID,
							"from_storage":   job.SourceNode.Storage,
							"to_storage":     job.TargetNode.Storage,
						}).WithError(err).Error("replication job failed")
						if err := r.datastore.UpdateReplJobState(job.ID, datastore.JobStateFailed); err != nil {
							r.log.WithError(err).Error("error when updating replication job status to failed")
						}
						continue
					}

					reposReplicated[replicationKey] = struct{}{}
				}
			}
		} else {
			r.log.WithError(err).WithField("virtual_storage", r.virtualStorage).Error("error when getting primary and secondaries")
		}

		if totalJobs == 0 {
			select {
			case <-time.After(backoff()):
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		reset()
	}
}

// TODO: errors that occur during replication should be handled better. Logging
// is a crutch in this situation. Ideally, we need to update state somewhere
// with information regarding the replication failure. See follow up issue:
// https://gitlab.com/gitlab-org/gitaly/issues/2138

func (r ReplMgr) processReplJob(ctx context.Context, job datastore.ReplJob, sourceCC, targetCC *grpc.ClientConn) error {
	l := r.log.
		WithField(logWithReplJobID, job.ID).
		WithField(logWithReplSource, job.SourceNode).
		WithField(logWithReplTarget, job.TargetNode)

	if err := r.datastore.UpdateReplJobState(job.ID, datastore.JobStateInProgress); err != nil {
		l.WithError(err).Error("unable to update replication job to in progress")
		return err
	}

	if err := r.datastore.IncrReplJobAttempts(job.ID); err != nil {
		l.WithError(err).Error("unable to increment replication job attempts")
		return err
	}

	var replCtx context.Context
	var cancel func()

	if r.replJobTimeout > 0 {
		replCtx, cancel = context.WithTimeout(ctx, r.replJobTimeout)
	} else {
		replCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	injectedCtx, err := helper.InjectGitalyServers(replCtx, job.SourceNode.Storage, job.SourceNode.Address, job.SourceNode.Token)
	if err != nil {
		l.WithError(err).Error("unable to inject Gitaly servers into context for replication job")
		return err
	}

	replStart := time.Now()
	r.replQueueMetric.Inc()
	defer r.replQueueMetric.Dec()

	switch job.Change {
	case datastore.UpdateRepo:
		err = r.replicator.Replicate(injectedCtx, job, sourceCC, targetCC)
	case datastore.DeleteRepo:
		err = r.replicator.Destroy(injectedCtx, job, targetCC)
	case datastore.RenameRepo:
		err = r.replicator.Rename(injectedCtx, job, targetCC)
	default:
		err = fmt.Errorf("unknown replication change type encountered: %d", job.Change)
	}
	if err != nil {
		l.WithError(err).Error("unable to replicate")
		return err
	}

	replDuration := time.Since(replStart)
	r.replLatencyMetric.Observe(float64(replDuration) / float64(time.Second))

	if err := r.datastore.UpdateReplJobState(job.ID, datastore.JobStateComplete); err != nil {
		r.log.WithError(err).Error("error when updating replication job status to complete")
	}

	return nil
}
