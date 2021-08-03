package backup

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

// Strategy used to create/restore backups
type Strategy interface {
	Create(context.Context, *CreateRequest) error
	Restore(context.Context, *RestoreRequest) error
}

// CreatePipeline is a pipeline that only handles creating backups
type CreatePipeline interface {
	Create(context.Context, *CreateRequest)
	Done() error
}

// Pipeline handles a series of requests to create/restore backups. Pipeline
// encapsulates error handling for the caller.
type Pipeline struct {
	log      logrus.FieldLogger
	strategy Strategy
	failed   int64
}

// NewPipeline creates a new pipeline
func NewPipeline(log logrus.FieldLogger, strategy Strategy) *Pipeline {
	return &Pipeline{
		log:      log,
		strategy: strategy,
	}
}

// Create requests that a repository backup be created
func (p *Pipeline) Create(ctx context.Context, req *CreateRequest) {
	repoLog := p.repoLogger(req.Repository)
	repoLog.Info("started backup")

	if err := p.strategy.Create(ctx, req); err != nil {
		if errors.Is(err, ErrSkipped) {
			repoLog.WithError(err).Warn("skipped backup")
		} else {
			repoLog.WithError(err).Error("backup failed")
			atomic.AddInt64(&p.failed, 1)
		}
		return
	}

	repoLog.Info("completed backup")
}

// Restore requests that a repository be restored from backup
func (p *Pipeline) Restore(ctx context.Context, req *RestoreRequest) {
	repoLog := p.repoLogger(req.Repository)
	repoLog.Info("started restore")

	if err := p.strategy.Restore(ctx, req); err != nil {
		if errors.Is(err, ErrSkipped) {
			repoLog.WithError(err).Warn("skipped restore")
		} else {
			repoLog.WithError(err).Error("restore failed")
			atomic.AddInt64(&p.failed, 1)
		}
		return
	}

	repoLog.Info("completed restore")
}

// Done indicates that the pipeline is complete and returns any accumulated errors
func (p *Pipeline) Done() error {
	if p.failed > 0 {
		return fmt.Errorf("pipeline: %d failures encountered", p.failed)
	}
	return nil
}

func (p *Pipeline) repoLogger(repo *gitalypb.Repository) logrus.FieldLogger {
	return p.log.WithFields(logrus.Fields{
		"storage_name":    repo.StorageName,
		"relative_path":   repo.RelativePath,
		"gl_project_path": repo.GlProjectPath,
	})
}

type contextCreateRequest struct {
	CreateRequest
	Context context.Context
}

// ParallelCreatePipeline is a pipeline that creates backups in parallel
type ParallelCreatePipeline struct {
	next            CreatePipeline
	parallel        int
	parallelStorage int

	wg          sync.WaitGroup
	workerSlots chan struct{}
	done        chan struct{}

	mu       sync.Mutex
	requests map[string]chan *contextCreateRequest
	err      error
}

// NewParallelCreatePipeline creates a new ParallelCreatePipeline
// where `next` is the pipeline called to create the backups, `parallel` is the
// maximum number of parallel backups that will run and `parallelStorage` is
// the maximum number of parallel backups that will run per storage. Since the
// number of storages is unknown at initialisation, workers are created lazily
// as new storage names are encountered.
//
// Note: When both `parallel` and `parallelStorage` are zero or less no workers
// are created and the pipeline will block forever.
func NewParallelCreatePipeline(next CreatePipeline, parallel, parallelStorage int) *ParallelCreatePipeline {
	var workerSlots chan struct{}
	if parallel > 0 && parallelStorage > 0 {
		// workerSlots allows the total number of parallel jobs to be
		// limited. This allows us to create the required workers for
		// each storage, while still limiting the absolute parallelism.
		workerSlots = make(chan struct{}, parallel)
	}
	return &ParallelCreatePipeline{
		next:            next,
		parallel:        parallel,
		parallelStorage: parallelStorage,
		workerSlots:     workerSlots,
		done:            make(chan struct{}),
		requests:        make(map[string]chan *contextCreateRequest),
	}
}

// Create queues a request to create a backup. Requests are processed by
// n-workers per storage.
func (p *ParallelCreatePipeline) Create(ctx context.Context, req *CreateRequest) {
	ch := p.getStorage(req.Repository.StorageName)

	select {
	case <-ctx.Done():
		p.setErr(ctx.Err())
	case ch <- &contextCreateRequest{
		CreateRequest: *req,
		Context:       ctx,
	}:
	}
}

// Done waits for any in progress calls to `Create` to complete then reports any accumulated errors
func (p *ParallelCreatePipeline) Done() error {
	close(p.done)
	p.wg.Wait()
	if err := p.next.Done(); err != nil {
		return err
	}
	if p.err != nil {
		return fmt.Errorf("pipeline: %w", p.err)
	}
	return nil
}

// getStorage finds the channel associated with a storage. When no channel is
// found, one is created and n-workers are started to process requests.
func (p *ParallelCreatePipeline) getStorage(storage string) chan<- *contextCreateRequest {
	p.mu.Lock()
	defer p.mu.Unlock()

	workers := p.parallelStorage

	if p.parallelStorage < 1 {
		// if the workers are not limited by storage, then pretend there is a single storage with `parallel` workers
		storage = ""
		workers = p.parallel
	}

	ch, ok := p.requests[storage]
	if !ok {
		ch = make(chan *contextCreateRequest)
		p.requests[storage] = ch

		for i := 0; i < workers; i++ {
			p.wg.Add(1)
			go p.worker(ch)
		}
	}
	return ch
}

func (p *ParallelCreatePipeline) worker(ch <-chan *contextCreateRequest) {
	defer p.wg.Done()
	for {
		select {
		case <-p.done:
			return
		case req := <-ch:
			p.processRequest(req.Context, &req.CreateRequest)
		}
	}
}

func (p *ParallelCreatePipeline) processRequest(ctx context.Context, req *CreateRequest) {
	p.acquireWorkerSlot()
	defer p.releaseWorkerSlot()

	p.next.Create(ctx, req)
}

func (p *ParallelCreatePipeline) setErr(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return
	}
	p.err = err
}

// acquireWorkerSlot queues the worker until a slot is available.
// It never blocks if `parallel` or `parallelStorage` are 0
func (p *ParallelCreatePipeline) acquireWorkerSlot() {
	if p.workerSlots == nil {
		return
	}
	p.workerSlots <- struct{}{}
}

// releaseWorkerSlot releases the worker slot.
func (p *ParallelCreatePipeline) releaseWorkerSlot() {
	if p.workerSlots == nil {
		return
	}
	<-p.workerSlots
}
