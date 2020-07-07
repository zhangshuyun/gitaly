package praefect

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/metadatahandler"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metrics"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"
	prommetrics "gitlab.com/gitlab-org/gitaly/internal/prometheus/metrics"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	grpccorrelation "gitlab.com/gitlab-org/labkit/correlation/grpc"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// Replicator performs the actual replication logic between two nodes
type Replicator interface {
	// Replicate propagates changes from the source to the target
	Replicate(ctx context.Context, event datastore.ReplicationEvent, source, target *grpc.ClientConn) error
	// Destroy will remove the target repo on the specified target connection
	Destroy(ctx context.Context, event datastore.ReplicationEvent, target *grpc.ClientConn) error
	// Rename will rename(move) the target repo on the specified target connection
	Rename(ctx context.Context, event datastore.ReplicationEvent, target *grpc.ClientConn) error
	// GarbageCollect will run gc on the target repository
	GarbageCollect(ctx context.Context, event datastore.ReplicationEvent, target *grpc.ClientConn) error
	// RepackFull will do a full repack on the target repository
	RepackFull(ctx context.Context, event datastore.ReplicationEvent, target *grpc.ClientConn) error
	// RepackIncremental will do an incremental repack on the target repository
	RepackIncremental(ctx context.Context, event datastore.ReplicationEvent, target *grpc.ClientConn) error
}

type defaultReplicator struct {
	log *logrus.Entry
	gs  datastore.GenerationStore
}

func (dr defaultReplicator) Replicate(ctx context.Context, event datastore.ReplicationEvent, sourceCC, targetCC *grpc.ClientConn) error {
	targetRepository := &gitalypb.Repository{
		StorageName:  event.Job.TargetNodeStorage,
		RelativePath: event.Job.RelativePath,
	}

	sourceRepository := &gitalypb.Repository{
		StorageName:  event.Job.SourceNodeStorage,
		RelativePath: event.Job.RelativePath,
	}

	// Old instances might produce replication jobs which do not have a generation number. Such replication jobs
	// are processed if the target repository does not have a known generation yet.
	generation := datastore.GenerationUnknown
	if evtGen, ok := event.Job.Params["generation"]; ok {
		generation = int(evtGen.(float64))
	}

	if err := dr.gs.EnsureUpgrade(ctx, event.Job.VirtualStorage, event.Job.TargetNodeStorage, event.Job.RelativePath, generation); err != nil {
		return fmt.Errorf("ensure upgrade: %w", err)
	}

	targetRepositoryClient := gitalypb.NewRepositoryServiceClient(targetCC)

	if _, err := targetRepositoryClient.ReplicateRepository(ctx, &gitalypb.ReplicateRepositoryRequest{
		Source:     sourceRepository,
		Repository: targetRepository,
	}); err != nil {
		return fmt.Errorf("failed to create repository: %v", err)
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
		metrics.ChecksumMismatchCounter.WithLabelValues(
			targetRepository.GetStorageName(),
			sourceRepository.GetStorageName(),
		).Inc()
		dr.log.WithFields(logrus.Fields{
			"primary": sourceRepository,
			"replica": targetRepository,
		}).Error("checksums do not match")
	}

	if generation >= 0 {
		return dr.gs.SetGeneration(ctx,
			event.Job.VirtualStorage,
			event.Job.TargetNodeStorage,
			event.Job.RelativePath,
			generation,
		)
	}

	return nil
}

func (dr defaultReplicator) Destroy(ctx context.Context, event datastore.ReplicationEvent, targetCC *grpc.ClientConn) error {
	targetRepo := &gitalypb.Repository{
		StorageName:  event.Job.TargetNodeStorage,
		RelativePath: event.Job.RelativePath,
	}

	repoSvcClient := gitalypb.NewRepositoryServiceClient(targetCC)

	// This might succeed and the repository removal fail. The repository removal will be reattempted as long
	// as the replication job is still alive. We'll need an additional field later to separate this case from
	// a missing repository record and to reattempt the removal even after the original replication job is dead.
	if err := dr.gs.DeleteRecord(ctx, event.Job.VirtualStorage, event.Job.TargetNodeStorage, event.Job.RelativePath); err != nil {
		return err
	}

	_, err := repoSvcClient.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{
		Repository: targetRepo,
	})

	return err
}

func (dr defaultReplicator) Rename(ctx context.Context, event datastore.ReplicationEvent, targetCC *grpc.ClientConn) error {
	targetRepo := &gitalypb.Repository{
		StorageName:  event.Job.TargetNodeStorage,
		RelativePath: event.Job.RelativePath,
	}

	repoSvcClient := gitalypb.NewRepositoryServiceClient(targetCC)

	val, found := event.Job.Params["RelativePath"]
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

func (dr defaultReplicator) GarbageCollect(ctx context.Context, event datastore.ReplicationEvent, targetCC *grpc.ClientConn) error {
	targetRepo := &gitalypb.Repository{
		StorageName:  event.Job.TargetNodeStorage,
		RelativePath: event.Job.RelativePath,
	}

	val, found := event.Job.Params["CreateBitmap"]
	if !found {
		return errors.New("no 'CreateBitmap' parameter for garbage collect")
	}
	createBitmap, ok := val.(bool)
	if !ok {
		return fmt.Errorf("parameter 'CreateBitmap' has unexpected type: %T", createBitmap)
	}

	repoSvcClient := gitalypb.NewRepositoryServiceClient(targetCC)

	_, err := repoSvcClient.GarbageCollect(ctx, &gitalypb.GarbageCollectRequest{
		Repository:   targetRepo,
		CreateBitmap: createBitmap,
	})

	return err
}

func (dr defaultReplicator) RepackIncremental(ctx context.Context, event datastore.ReplicationEvent, targetCC *grpc.ClientConn) error {
	targetRepo := &gitalypb.Repository{
		StorageName:  event.Job.TargetNodeStorage,
		RelativePath: event.Job.RelativePath,
	}

	repoSvcClient := gitalypb.NewRepositoryServiceClient(targetCC)

	_, err := repoSvcClient.RepackIncremental(ctx, &gitalypb.RepackIncrementalRequest{
		Repository: targetRepo,
	})

	return err
}

func (dr defaultReplicator) RepackFull(ctx context.Context, event datastore.ReplicationEvent, targetCC *grpc.ClientConn) error {
	targetRepo := &gitalypb.Repository{
		StorageName:  event.Job.TargetNodeStorage,
		RelativePath: event.Job.RelativePath,
	}

	val, found := event.Job.Params["CreateBitmap"]
	if !found {
		return errors.New("no 'CreateBitmap' parameter for repack full")
	}
	createBitmap, ok := val.(bool)
	if !ok {
		return fmt.Errorf("parameter 'CreateBitmap' has unexpected type: %T", createBitmap)
	}

	repoSvcClient := gitalypb.NewRepositoryServiceClient(targetCC)

	_, err := repoSvcClient.RepackFull(ctx, &gitalypb.RepackFullRequest{
		Repository:   targetRepo,
		CreateBitmap: createBitmap,
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

// StorageGauge is used to track the in-flight replication jobs by virtual and
// Gitaly target storage
type StorageGauge interface {
	Inc(virtualStorage, gitalyStorage string)
	Dec(virtualStorage, gitalyStorage string)
}

// ReplMgr is a replication manager for handling replication jobs
type ReplMgr struct {
	log                *logrus.Entry
	queue              datastore.ReplicationEventQueue
	nodeManager        nodes.Manager
	virtualStorages    []string   // replicas this replicator is responsible for
	replicator         Replicator // does the actual replication logic
	replInFlightMetric StorageGauge
	replLatencyMetric  prommetrics.HistogramVec
	replDelayMetric    prommetrics.HistogramVec
	replJobTimeout     time.Duration
	// whitelist contains the project names of the repos we wish to replicate
	whitelist map[string]struct{}
}

// ReplMgrOpt allows a replicator to be configured with additional options
type ReplMgrOpt func(*ReplMgr)

// WithInFlightJobsGauge is an option to set the replication jobs in-flight
// gauge
func WithInFlightJobsGauge(sg StorageGauge) ReplMgrOpt {
	return func(m *ReplMgr) {
		m.replInFlightMetric = sg
	}
}

// WithLatencyMetric is an option to set the latency prometheus metric
func WithLatencyMetric(h prommetrics.HistogramVec) func(*ReplMgr) {
	return func(m *ReplMgr) {
		m.replLatencyMetric = h
	}
}

// WithDelayMetric is an option to set the delay prometheus metric
func WithDelayMetric(h prommetrics.HistogramVec) func(*ReplMgr) {
	return func(m *ReplMgr) {
		m.replDelayMetric = h
	}
}

// NewReplMgr initializes a replication manager with the provided dependencies
// and options
func NewReplMgr(log *logrus.Entry, virtualStorages []string, queue datastore.ReplicationEventQueue, gs datastore.GenerationStore, nodeMgr nodes.Manager, opts ...ReplMgrOpt) ReplMgr {
	r := ReplMgr{
		log:               log.WithField("component", "replication_manager"),
		queue:             queue,
		whitelist:         map[string]struct{}{},
		replicator:        defaultReplicator{log, gs},
		virtualStorages:   virtualStorages,
		nodeManager:       nodeMgr,
		replLatencyMetric: prometheus.NewHistogramVec(prometheus.HistogramOpts{}, []string{"type"}),
		replDelayMetric:   prometheus.NewHistogramVec(prometheus.HistogramOpts{}, []string{"type"}),
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
	logWithReplJobID   = "replication_job_id"
	logWithReplVirtual = "replication_job_virtual"
	logWithReplSource  = "replication_job_source"
	logWithReplTarget  = "replication_job_target"
	logWithReplChange  = "replication_job_change"
	logWithReplPath    = "replication_job_path"
	logWithCorrID      = "replication_correlation_id"
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

func getCorrelationID(params datastore.Params) string {
	correlationID := ""
	if val, found := params[metadatahandler.CorrelationIDKey]; found {
		correlationID, _ = val.(string)
	}
	return correlationID
}

// ProcessBacklog starts processing of queued jobs.
// It will be processing jobs until ctx is Done.
func (r ReplMgr) ProcessBacklog(ctx context.Context, b BackoffFunc) {
	for _, virtualStorage := range r.virtualStorages {
		go r.processBacklog(ctx, b, virtualStorage)
	}
}

func (r ReplMgr) processBacklog(ctx context.Context, b BackoffFunc, virtualStorage string) {
	logger := r.log.WithField("virtual_storage", virtualStorage)
	backoff, reset := b()

	logger.Info("processing started")

	for {
		select {
		case <-ctx.Done():
			logger.WithError(ctx.Err()).Info("processing stopped")
			return // processing must be stopped
		default:
			// proceed with processing
		}

		var totalEvents int
		shard, err := r.nodeManager.GetShard(virtualStorage)
		if err != nil {
			logger.WithError(err).Error("error when getting primary and secondaries")
		} else {
			targetNodes := shard.Secondaries
			if shard.IsReadOnly {
				targetNodes = append(targetNodes, shard.Primary)
			}

			for _, target := range targetNodes {
				totalEvents += r.handleNode(ctx, logger, shard, virtualStorage, target)
			}
		}

		if totalEvents == 0 {
			select {
			case <-time.After(backoff()):
				continue
			case <-ctx.Done():
				logger.WithError(ctx.Err()).Info("processing stopped")
				return
			}
		}

		reset()
	}
}

func (r ReplMgr) handleNode(ctx context.Context, logger logrus.FieldLogger, shard nodes.Shard, virtualStorage string, target nodes.Node) int {
	events, err := r.queue.Dequeue(ctx, virtualStorage, target.GetStorage(), 10)
	if err != nil {
		logger.WithField(logWithReplTarget, target.GetStorage()).WithError(err).Error("failed to dequeue replication events")
		return 0
	}

	if len(events) == 0 {
		return 0
	}

	stopHealthUpdate := r.startHealthUpdate(ctx, logger, events)
	defer stopHealthUpdate()

	eventIDsByState := map[datastore.JobState][]uint64{}
	for _, event := range events {
		state := r.handleNodeEvent(ctx, logger, shard, target, event)
		eventIDsByState[state] = append(eventIDsByState[state], event.ID)
	}

	for state, eventIDs := range eventIDsByState {
		ackIDs, err := r.queue.Acknowledge(ctx, state, eventIDs)
		if err != nil {
			logger.WithField("state", state).WithField("event_ids", eventIDs).WithError(err).Error("failed to acknowledge replication events")
			continue
		}

		notAckIDs := subtractUint64(ackIDs, eventIDs)
		if len(notAckIDs) > 0 {
			logger.WithField("state", state).WithField("event_ids", notAckIDs).WithError(err).Error("replication events were not acknowledged")
		}
	}

	return len(events)
}

func (r ReplMgr) startHealthUpdate(ctx context.Context, logger logrus.FieldLogger, events []datastore.ReplicationEvent) context.CancelFunc {
	healthUpdateCtx, healthUpdateCancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		if err := r.queue.StartHealthUpdate(healthUpdateCtx, ticker.C, events); err != nil {
			ids := make([]uint64, len(events))
			for i, event := range events {
				ids[i] = event.ID
			}

			logger.WithField("event_ids", ids).WithError(err).Error("health update loop")
		}
	}()

	return healthUpdateCancel
}

func (r ReplMgr) handleNodeEvent(ctx context.Context, logger logrus.FieldLogger, shard nodes.Shard, target nodes.Node, event datastore.ReplicationEvent) datastore.JobState {
	logger = logger.WithFields(logrus.Fields{
		logWithReplJobID: event.ID,
		logWithCorrID:    getCorrelationID(event.Meta),
	})
	logger.Info("processing replication job")
	err := r.processReplicationEvent(ctx, event, shard, target.GetConnection())
	if err == nil {
		return datastore.JobStateCompleted
	}

	logger.WithError(err).Error("replication job failed")

	if event.Attempt <= 0 {
		return datastore.JobStateDead
	}

	return datastore.JobStateFailed
}

func (r ReplMgr) processReplicationEvent(ctx context.Context, event datastore.ReplicationEvent, shard nodes.Shard, targetCC *grpc.ClientConn) error {
	source, err := shard.GetNode(event.Job.SourceNodeStorage)
	if err != nil {
		return fmt.Errorf("get source node: %w", err)
	}

	cid := getCorrelationID(event.Meta)

	var replCtx context.Context
	var cancel func()

	if r.replJobTimeout > 0 {
		replCtx, cancel = context.WithTimeout(ctx, r.replJobTimeout)
	} else {
		replCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	injectedCtx, err := helper.InjectGitalyServers(replCtx, event.Job.SourceNodeStorage, source.GetAddress(), source.GetToken())
	if err != nil {
		return fmt.Errorf("inject Gitaly servers into context: %w", err)
	}
	injectedCtx = grpccorrelation.InjectToOutgoingContext(injectedCtx, cid)

	replStart := time.Now()

	r.replDelayMetric.WithLabelValues(event.Job.Change.String()).Observe(replStart.Sub(event.CreatedAt).Seconds())

	if r.replInFlightMetric != nil {
		r.replInFlightMetric.Inc(event.Job.VirtualStorage, event.Job.TargetNodeStorage)
		defer r.replInFlightMetric.Dec(event.Job.VirtualStorage, event.Job.TargetNodeStorage)
	}

	switch event.Job.Change {
	case datastore.UpdateRepo:
		err = r.replicator.Replicate(injectedCtx, event, source.GetConnection(), targetCC)
	case datastore.DeleteRepo:
		err = r.replicator.Destroy(injectedCtx, event, targetCC)
	case datastore.RenameRepo:
		err = r.replicator.Rename(injectedCtx, event, targetCC)
	case datastore.GarbageCollect:
		err = r.replicator.GarbageCollect(injectedCtx, event, targetCC)
	case datastore.RepackFull:
		err = r.replicator.RepackFull(injectedCtx, event, targetCC)
	case datastore.RepackIncremental:
		err = r.replicator.RepackIncremental(injectedCtx, event, targetCC)
	default:
		err = fmt.Errorf("unknown replication change type encountered: %q", event.Job.Change)
	}
	if err != nil {
		return err
	}

	r.replLatencyMetric.WithLabelValues(event.Job.Change.String()).Observe(time.Since(replStart).Seconds())

	return nil
}

// subtractUint64 returns new slice that has all elements from left that does not exist at right.
func subtractUint64(l, r []uint64) []uint64 {
	if len(l) == 0 {
		return nil
	}

	if len(r) == 0 {
		result := make([]uint64, len(l))
		copy(result, l)
		return result
	}

	excludeSet := make(map[uint64]struct{}, len(l))
	for _, v := range r {
		excludeSet[v] = struct{}{}
	}

	var result []uint64
	for _, v := range l {
		if _, found := excludeSet[v]; !found {
			result = append(result, v)
		}
	}

	return result
}
