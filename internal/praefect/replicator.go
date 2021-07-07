package praefect

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/middleware/metadatahandler"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	prommetrics "gitlab.com/gitlab-org/gitaly/v14/internal/prometheus/metrics"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/labkit/correlation"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
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
	// Cleanup will do a cleanup on the target repository
	Cleanup(ctx context.Context, event datastore.ReplicationEvent, target *grpc.ClientConn) error
	// PackRefs will optimize references on the target repository
	PackRefs(ctx context.Context, event datastore.ReplicationEvent, target *grpc.ClientConn) error
	// WriteCommitGraph will optimize references on the target repository
	WriteCommitGraph(ctx context.Context, event datastore.ReplicationEvent, target *grpc.ClientConn) error
	// MidxRepack will optimize references on the target repository
	MidxRepack(ctx context.Context, event datastore.ReplicationEvent, target *grpc.ClientConn) error
	// OptimizeRepository will optimize the target repository
	OptimizeRepository(ctx context.Context, event datastore.ReplicationEvent, target *grpc.ClientConn) error
}

type defaultReplicator struct {
	rs  datastore.RepositoryStore
	log logrus.FieldLogger
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

	logger := dr.log.WithFields(logrus.Fields{
		logWithVirtualStorage:    event.Job.VirtualStorage,
		logWithReplTarget:        event.Job.TargetNodeStorage,
		"replication_job_source": event.Job.SourceNodeStorage,
		logWithCorrID:            correlation.ExtractFromContext(ctx),
	})

	generation, err := dr.rs.GetReplicatedGeneration(ctx, event.Job.VirtualStorage, event.Job.RelativePath, event.Job.SourceNodeStorage, event.Job.TargetNodeStorage)
	if err != nil {
		// Later generation might have already been replicated by an earlier replication job. If that's the case,
		// we'll simply acknowledge the job. This also prevents accidental downgrades from happening.
		var downgradeErr datastore.DowngradeAttemptedError
		if errors.As(err, &downgradeErr) {
			message := "repository downgrade prevented"
			if downgradeErr.CurrentGeneration == downgradeErr.AttemptedGeneration {
				message = "target repository already on the same generation, skipping replication job"
			}

			logger.WithError(downgradeErr).Info(message)
			return nil
		}

		return fmt.Errorf("get replicated generation: %w", err)
	}

	targetRepositoryClient := gitalypb.NewRepositoryServiceClient(targetCC)

	if _, err := targetRepositoryClient.ReplicateRepository(ctx, &gitalypb.ReplicateRepositoryRequest{
		Source:     sourceRepository,
		Repository: targetRepository,
	}); err != nil {
		if errors.Is(err, repository.ErrInvalidSourceRepository) {
			if err := dr.rs.DeleteInvalidRepository(ctx,
				event.Job.VirtualStorage,
				event.Job.RelativePath,
				event.Job.SourceNodeStorage,
			); err != nil {
				return fmt.Errorf("delete invalid repository: %w", err)
			}

			logger.Info("invalid repository record removed")
			return nil
		}

		return fmt.Errorf("failed to create repository: %w", err)
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
		targetObjectPool := proto.Clone(sourceObjectPool).(*gitalypb.ObjectPool)
		targetObjectPool.GetRepository().StorageName = targetRepository.GetStorageName()
		if _, err := targetObjectPoolClient.LinkRepositoryToObjectPool(ctx, &gitalypb.LinkRepositoryToObjectPoolRequest{
			ObjectPool: targetObjectPool,
			Repository: targetRepository,
		}); err != nil {
			return err
		}
	}

	if generation != datastore.GenerationUnknown {
		return dr.rs.SetGeneration(ctx,
			event.Job.VirtualStorage,
			event.Job.RelativePath,
			event.Job.TargetNodeStorage,
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

	if _, err := repoSvcClient.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{
		Repository: targetRepo,
	}); err != nil {
		return err
	}

	var deleteFunc func(context.Context, string, string, string) error
	switch event.Job.Change {
	case datastore.DeleteRepo:
		deleteFunc = dr.rs.DeleteRepository
	case datastore.DeleteReplica:
		deleteFunc = dr.rs.DeleteReplica
	default:
		return fmt.Errorf("unknown change type: %q", event.Job.Change)
	}

	// If the repository was deleted but this fails, we'll know by the repository not having a record in the virtual
	// storage but having one for the storage. We can later retry the deletion.
	if err := deleteFunc(ctx, event.Job.VirtualStorage, event.Job.RelativePath, event.Job.TargetNodeStorage); err != nil {
		if !errors.Is(err, datastore.RepositoryNotExistsError{}) {
			return err
		}

		dr.log.WithField(logWithCorrID, correlation.ExtractFromContext(ctx)).
			WithError(err).
			Info("deleted repository did not have a store entry")
	}

	return nil
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

	if _, err := repoSvcClient.RenameRepository(ctx, &gitalypb.RenameRepositoryRequest{
		Repository:   targetRepo,
		RelativePath: relativePath,
	}); err != nil {
		return err
	}

	// If the repository was moved but this fails, we'll have a stale record on the storage but it is missing from the
	// virtual storage. We can later schedule a deletion to fix the situation. The newly named repository's record
	// will be present once a replication job arrives for it.
	if err := dr.rs.RenameRepository(ctx,
		event.Job.VirtualStorage, event.Job.RelativePath, event.Job.TargetNodeStorage, relativePath); err != nil {
		if !errors.Is(err, datastore.RepositoryNotExistsError{}) {
			return err
		}

		dr.log.WithField(logWithCorrID, correlation.ExtractFromContext(ctx)).
			WithError(err).
			Info("replicated repository rename does not have a store entry")
	}

	return nil
}

func (dr defaultReplicator) GarbageCollect(ctx context.Context, event datastore.ReplicationEvent, targetCC *grpc.ClientConn) error {
	targetRepo := &gitalypb.Repository{
		StorageName:  event.Job.TargetNodeStorage,
		RelativePath: event.Job.RelativePath,
	}

	createBitmap, err := event.Job.Params.GetBool("CreateBitmap")
	if err != nil {
		return fmt.Errorf("getting CreateBitmap parameter for GarbageCollect: %w", err)
	}

	prune, err := event.Job.Params.GetBool("Prune")
	if err != nil {
		return fmt.Errorf("getting Purge parameter for GarbageCollect: %w", err)
	}

	repoSvcClient := gitalypb.NewRepositoryServiceClient(targetCC)

	if _, err := repoSvcClient.GarbageCollect(ctx, &gitalypb.GarbageCollectRequest{
		Repository:   targetRepo,
		CreateBitmap: createBitmap,
		Prune:        prune,
	}); err != nil {
		return err
	}

	return nil
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

func (dr defaultReplicator) Cleanup(ctx context.Context, event datastore.ReplicationEvent, targetCC *grpc.ClientConn) error {
	targetRepo := &gitalypb.Repository{
		StorageName:  event.Job.TargetNodeStorage,
		RelativePath: event.Job.RelativePath,
	}

	repoSvcClient := gitalypb.NewRepositoryServiceClient(targetCC)

	_, err := repoSvcClient.Cleanup(ctx, &gitalypb.CleanupRequest{
		Repository: targetRepo,
	})

	return err
}

func (dr defaultReplicator) PackRefs(ctx context.Context, event datastore.ReplicationEvent, targetCC *grpc.ClientConn) error {
	targetRepo := &gitalypb.Repository{
		StorageName:  event.Job.TargetNodeStorage,
		RelativePath: event.Job.RelativePath,
	}

	allRefs, err := event.Job.Params.GetBool("AllRefs")
	if err != nil {
		return fmt.Errorf("getting AllRefs parameter for PackRefs: %w", err)
	}

	refSvcClient := gitalypb.NewRefServiceClient(targetCC)

	if _, err := refSvcClient.PackRefs(ctx, &gitalypb.PackRefsRequest{
		Repository: targetRepo,
		AllRefs:    allRefs,
	}); err != nil {
		return err
	}

	return nil
}

func (dr defaultReplicator) WriteCommitGraph(ctx context.Context, event datastore.ReplicationEvent, targetCC *grpc.ClientConn) error {
	targetRepo := &gitalypb.Repository{
		StorageName:  event.Job.TargetNodeStorage,
		RelativePath: event.Job.RelativePath,
	}

	val, found := event.Job.Params["SplitStrategy"]
	if !found {
		return fmt.Errorf("no SplitStrategy parameter for WriteCommitGraph")
	}

	// While we store the parameter as the correct type in the in-memory replication queue, the
	// Postgres queue will serialize parameters into a JSON structure. On deserialization, we'll
	// thus get a float64 and need to cast it.
	var splitStrategy gitalypb.WriteCommitGraphRequest_SplitStrategy
	switch v := val.(type) {
	case float64:
		splitStrategy = gitalypb.WriteCommitGraphRequest_SplitStrategy(v)
	case gitalypb.WriteCommitGraphRequest_SplitStrategy:
		splitStrategy = v
	default:
		return fmt.Errorf("split strategy has wrong type %T", val)
	}

	repoSvcClient := gitalypb.NewRepositoryServiceClient(targetCC)

	if _, err := repoSvcClient.WriteCommitGraph(ctx, &gitalypb.WriteCommitGraphRequest{
		Repository:    targetRepo,
		SplitStrategy: splitStrategy,
	}); err != nil {
		return err
	}

	return nil
}

func (dr defaultReplicator) MidxRepack(ctx context.Context, event datastore.ReplicationEvent, targetCC *grpc.ClientConn) error {
	targetRepo := &gitalypb.Repository{
		StorageName:  event.Job.TargetNodeStorage,
		RelativePath: event.Job.RelativePath,
	}

	repoSvcClient := gitalypb.NewRepositoryServiceClient(targetCC)

	if _, err := repoSvcClient.MidxRepack(ctx, &gitalypb.MidxRepackRequest{
		Repository: targetRepo,
	}); err != nil {
		return err
	}

	return nil
}

func (dr defaultReplicator) OptimizeRepository(ctx context.Context, event datastore.ReplicationEvent, targetCC *grpc.ClientConn) error {
	targetRepo := &gitalypb.Repository{
		StorageName:  event.Job.TargetNodeStorage,
		RelativePath: event.Job.RelativePath,
	}

	repoSvcClient := gitalypb.NewRepositoryServiceClient(targetCC)

	if _, err := repoSvcClient.OptimizeRepository(ctx, &gitalypb.OptimizeRepositoryRequest{
		Repository: targetRepo,
	}); err != nil {
		return err
	}

	return nil
}

func (dr defaultReplicator) RepackFull(ctx context.Context, event datastore.ReplicationEvent, targetCC *grpc.ClientConn) error {
	targetRepo := &gitalypb.Repository{
		StorageName:  event.Job.TargetNodeStorage,
		RelativePath: event.Job.RelativePath,
	}

	createBitmap, err := event.Job.Params.GetBool("CreateBitmap")
	if err != nil {
		return fmt.Errorf("getting CreateBitmap parameter for RepackFull: %w", err)
	}

	repoSvcClient := gitalypb.NewRepositoryServiceClient(targetCC)

	if _, err := repoSvcClient.RepackFull(ctx, &gitalypb.RepackFullRequest{
		Repository:   targetRepo,
		CreateBitmap: createBitmap,
	}); err != nil {
		return err
	}

	return nil
}

// ReplMgr is a replication manager for handling replication jobs
type ReplMgr struct {
	log                *logrus.Entry
	queue              datastore.ReplicationEventQueue
	hc                 HealthChecker
	nodes              NodeSet
	virtualStorages    []string   // replicas this replicator is responsible for
	replicator         Replicator // does the actual replication logic
	replInFlightMetric *prometheus.GaugeVec
	replLatencyMetric  prommetrics.HistogramVec
	replDelayMetric    prommetrics.HistogramVec
	replJobTimeout     time.Duration
	dequeueBatchSize   uint
	// allowlist contains the project names of the repos we wish to replicate
	allowlist map[string]struct{}
}

// ReplMgrOpt allows a replicator to be configured with additional options
type ReplMgrOpt func(*ReplMgr)

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

// WithDequeueBatchSize configures the number of events to dequeue in a single batch.
func WithDequeueBatchSize(size uint) func(*ReplMgr) {
	return func(m *ReplMgr) {
		m.dequeueBatchSize = size
	}
}

// NewReplMgr initializes a replication manager with the provided dependencies
// and options
func NewReplMgr(log *logrus.Entry, virtualStorages []string, queue datastore.ReplicationEventQueue, rs datastore.RepositoryStore, hc HealthChecker, nodes NodeSet, opts ...ReplMgrOpt) ReplMgr {
	r := ReplMgr{
		log:             log.WithField("component", "replication_manager"),
		queue:           queue,
		allowlist:       map[string]struct{}{},
		replicator:      defaultReplicator{rs: rs, log: log.WithField("component", "replicator")},
		virtualStorages: virtualStorages,
		hc:              hc,
		nodes:           nodes,
		replInFlightMetric: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "gitaly_praefect_replication_jobs",
				Help: "Number of replication jobs in flight.",
			}, []string{"virtual_storage", "gitaly_storage", "change_type"},
		),
		replLatencyMetric: prometheus.NewHistogramVec(prometheus.HistogramOpts{}, []string{"type"}),
		replDelayMetric:   prometheus.NewHistogramVec(prometheus.HistogramOpts{}, []string{"type"}),
		dequeueBatchSize:  config.DefaultReplicationConfig().BatchSize,
	}

	for _, opt := range opts {
		opt(&r)
	}

	return r
}

func (r ReplMgr) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(r, ch)
}

func (r ReplMgr) Collect(ch chan<- prometheus.Metric) {
	r.replInFlightMetric.Collect(ch)
}

// WithAllowlist will configure a allowlist for repos to allow replication
func WithAllowlist(allowlistedRepos []string) ReplMgrOpt {
	return func(r *ReplMgr) {
		for _, repo := range allowlistedRepos {
			r.allowlist[repo] = struct{}{}
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
	logWithReplTarget     = "replication_job_target"
	logWithCorrID         = "correlation_id"
	logWithVirtualStorage = "virtual_storage"
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
// It will be processing jobs until ctx is Done. ProcessBacklog
// blocks until all backlog processing goroutines have returned
func (r ReplMgr) ProcessBacklog(ctx context.Context, b BackoffFunc) {
	var wg sync.WaitGroup

	for _, virtualStorage := range r.virtualStorages {
		wg.Add(1)
		go func(virtualStorage string) {
			defer wg.Done()
			r.processBacklog(ctx, b, virtualStorage)
		}(virtualStorage)
	}

	wg.Wait()
}

// ProcessStale starts a background process to acknowledge stale replication jobs.
// It will process jobs until ctx is Done.
func (r ReplMgr) ProcessStale(ctx context.Context, checkPeriod, staleAfter time.Duration) chan struct{} {
	done := make(chan struct{})

	go func() {
		defer close(done)

		t := time.NewTimer(checkPeriod)
		for {
			select {
			case <-t.C:
				if err := r.queue.AcknowledgeStale(ctx, staleAfter); err != nil {
					r.log.WithError(err).Error("background periodical acknowledgement for stale replication jobs")
				}
				t.Reset(checkPeriod)
			case <-ctx.Done():
				return
			}
		}
	}()

	return done
}

func (r ReplMgr) processBacklog(ctx context.Context, b BackoffFunc, virtualStorage string) {
	logger := r.log.WithField(logWithVirtualStorage, virtualStorage)
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
		for _, storage := range r.hc.HealthyNodes()[virtualStorage] {
			target, ok := r.nodes[virtualStorage][storage]
			if !ok {
				logger.WithField("storage", storage).Error("no connection to target storage")
				continue
			}

			totalEvents += r.handleNode(ctx, virtualStorage, target)
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

func (r ReplMgr) handleNode(ctx context.Context, virtualStorage string, target Node) int {
	logger := r.log.WithFields(logrus.Fields{logWithVirtualStorage: virtualStorage, logWithReplTarget: target.Storage})

	events, err := r.queue.Dequeue(ctx, virtualStorage, target.Storage, int(r.dequeueBatchSize))
	if err != nil {
		logger.WithError(err).Error("failed to dequeue replication events")
		return 0
	}

	if len(events) == 0 {
		return 0
	}

	stopHealthUpdate := r.startHealthUpdate(ctx, logger, events)
	defer stopHealthUpdate()

	eventIDsByState := map[datastore.JobState][]uint64{}
	for _, event := range events {
		state := r.handleNodeEvent(ctx, logger, target.Connection, event)
		eventIDsByState[state] = append(eventIDsByState[state], event.ID)
	}

	for state, eventIDs := range eventIDsByState {
		ackIDs, err := r.queue.Acknowledge(ctx, state, eventIDs)
		if err != nil {
			logger.WithFields(logrus.Fields{"state": state, "event_ids": eventIDs}).
				WithError(err).
				Error("failed to acknowledge replication events")
			continue
		}

		notAckIDs := subtractUint64(ackIDs, eventIDs)
		if len(notAckIDs) > 0 {
			logger.WithFields(logrus.Fields{"state": state, "event_ids": notAckIDs}).
				WithError(err).
				Error("replication events were not acknowledged")
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

func (r ReplMgr) handleNodeEvent(ctx context.Context, logger logrus.FieldLogger, targetConnection *grpc.ClientConn, event datastore.ReplicationEvent) datastore.JobState {
	cid := getCorrelationID(event.Meta)
	ctx = correlation.ContextWithCorrelation(ctx, cid)

	// we want it to be queryable by common `json.correlation_id` filter
	logger = logger.WithField(logWithCorrID, cid)
	// we log all details about the event only once before start of the processing
	logger.WithField("event", event).Info("replication job processing started")

	if err := r.processReplicationEvent(ctx, event, targetConnection); err != nil {
		newState := datastore.JobStateFailed
		if event.Attempt <= 0 {
			newState = datastore.JobStateDead
		}

		logger.WithError(err).WithField("new_state", newState).Error("replication job processing finished")
		return newState
	}

	newState := datastore.JobStateCompleted
	logger.WithField("new_state", newState).Info("replication job processing finished")
	return newState
}

func (r ReplMgr) processReplicationEvent(ctx context.Context, event datastore.ReplicationEvent, targetCC *grpc.ClientConn) error {
	var cancel func()

	if r.replJobTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, r.replJobTimeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	replStart := time.Now()

	r.replDelayMetric.WithLabelValues(event.Job.Change.String()).Observe(replStart.Sub(event.CreatedAt).Seconds())

	inFlightGauge := r.replInFlightMetric.WithLabelValues(event.Job.VirtualStorage, event.Job.TargetNodeStorage, event.Job.Change.String())
	inFlightGauge.Inc()
	defer inFlightGauge.Dec()

	var err error
	switch event.Job.Change {
	case datastore.UpdateRepo:
		source, ok := r.nodes[event.Job.VirtualStorage][event.Job.SourceNodeStorage]
		if !ok {
			return fmt.Errorf("no connection to source node %q/%q", event.Job.VirtualStorage, event.Job.SourceNodeStorage)
		}

		ctx, err = helper.InjectGitalyServers(ctx, event.Job.SourceNodeStorage, source.Address, source.Token)
		if err != nil {
			return fmt.Errorf("inject Gitaly servers into context: %w", err)
		}

		err = r.replicator.Replicate(ctx, event, source.Connection, targetCC)
	case datastore.DeleteRepo, datastore.DeleteReplica:
		err = r.replicator.Destroy(ctx, event, targetCC)
	case datastore.RenameRepo:
		err = r.replicator.Rename(ctx, event, targetCC)
	case datastore.GarbageCollect:
		err = r.replicator.GarbageCollect(ctx, event, targetCC)
	case datastore.RepackFull:
		err = r.replicator.RepackFull(ctx, event, targetCC)
	case datastore.RepackIncremental:
		err = r.replicator.RepackIncremental(ctx, event, targetCC)
	case datastore.Cleanup:
		err = r.replicator.Cleanup(ctx, event, targetCC)
	case datastore.PackRefs:
		err = r.replicator.PackRefs(ctx, event, targetCC)
	case datastore.WriteCommitGraph:
		err = r.replicator.WriteCommitGraph(ctx, event, targetCC)
	case datastore.MidxRepack:
		err = r.replicator.MidxRepack(ctx, event, targetCC)
	case datastore.OptimizeRepository:
		err = r.replicator.OptimizeRepository(ctx, event, targetCC)
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
