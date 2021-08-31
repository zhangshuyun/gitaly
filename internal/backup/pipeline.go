package backup

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

// Strategy used to create/restore backups
type Strategy interface {
	Create(context.Context, *CreateRequest) error
	Restore(context.Context, *RestoreRequest) error
}

// Command handles a specific backup operation
type Command interface {
	Repository() *gitalypb.Repository
	Name() string
	Execute(context.Context) error
}

// Pipeline executes a series of commands and encapsulates error handling for
// the caller.
type Pipeline interface {
	Handle(context.Context, Command)
	Done() error
}

// CreateCommand creates a backup for a repository
type CreateCommand struct {
	strategy   Strategy
	server     storage.ServerInfo
	repository *gitalypb.Repository
}

// NewCreateCommand builds a CreateCommand
func NewCreateCommand(strategy Strategy, server storage.ServerInfo, repo *gitalypb.Repository) *CreateCommand {
	return &CreateCommand{
		strategy:   strategy,
		server:     server,
		repository: repo,
	}
}

// Repository is the repository that will be acted on
func (cmd CreateCommand) Repository() *gitalypb.Repository {
	return cmd.repository
}

// Name is the name of the command
func (cmd CreateCommand) Name() string {
	return "create"
}

// Execute performs the backup
func (cmd CreateCommand) Execute(ctx context.Context) error {
	return cmd.strategy.Create(ctx, &CreateRequest{
		Server:     cmd.server,
		Repository: cmd.repository,
	})
}

// RestoreCommand restores a backup for a repository
type RestoreCommand struct {
	strategy     Strategy
	server       storage.ServerInfo
	repository   *gitalypb.Repository
	alwaysCreate bool
}

// NewRestoreCommand builds a RestoreCommand
func NewRestoreCommand(strategy Strategy, server storage.ServerInfo, repo *gitalypb.Repository, alwaysCreate bool) *RestoreCommand {
	return &RestoreCommand{
		strategy:     strategy,
		server:       server,
		repository:   repo,
		alwaysCreate: alwaysCreate,
	}
}

// Repository is the repository that will be acted on
func (cmd RestoreCommand) Repository() *gitalypb.Repository {
	return cmd.repository
}

// Name is the name of the command
func (cmd RestoreCommand) Name() string {
	return "restore"
}

// Execute performs the restore
func (cmd RestoreCommand) Execute(ctx context.Context) error {
	return cmd.strategy.Restore(ctx, &RestoreRequest{
		Server:       cmd.server,
		Repository:   cmd.repository,
		AlwaysCreate: cmd.alwaysCreate,
	})
}

// LoggingPipeline outputs logging for each command executed
type LoggingPipeline struct {
	log    logrus.FieldLogger
	failed int64
}

// NewLoggingPipeline creates a new logging pipeline
func NewLoggingPipeline(log logrus.FieldLogger) *LoggingPipeline {
	return &LoggingPipeline{
		log: log,
	}
}

// Handle takes a command to process. Commands are logged and executed immediately.
func (p *LoggingPipeline) Handle(ctx context.Context, cmd Command) {
	log := p.cmdLogger(cmd)
	log.Infof("started %s", cmd.Name())

	if err := cmd.Execute(ctx); err != nil {
		if errors.Is(err, ErrSkipped) {
			log.WithError(err).Warnf("skipped %s", cmd.Name())
		} else {
			log.WithError(err).Errorf("%s failed", cmd.Name())
			atomic.AddInt64(&p.failed, 1)
		}
		return
	}

	log.Infof("completed %s", cmd.Name())
}

// Done indicates that the pipeline is complete and returns any accumulated errors
func (p *LoggingPipeline) Done() error {
	if p.failed > 0 {
		return fmt.Errorf("pipeline: %d failures encountered", p.failed)
	}
	return nil
}

func (p *LoggingPipeline) cmdLogger(cmd Command) logrus.FieldLogger {
	return p.log.WithFields(logrus.Fields{
		"command":         cmd.Name(),
		"storage_name":    cmd.Repository().StorageName,
		"relative_path":   cmd.Repository().RelativePath,
		"gl_project_path": cmd.Repository().GlProjectPath,
	})
}

type contextCommand struct {
	Command Command
	Context context.Context
}

// ParallelPipeline is a pipeline that executes commands in parallel
type ParallelPipeline struct {
	next            Pipeline
	parallel        int
	parallelStorage int

	wg          sync.WaitGroup
	workerSlots chan struct{}
	done        chan struct{}

	mu       sync.Mutex
	requests map[string]chan *contextCommand
	err      error
}

// NewParallelPipeline creates a new ParallelPipeline where all commands are
// passed onto `next` to be processed, `parallel` is the maximum number of
// parallel backups that will run and `parallelStorage` is the maximum number
// of parallel backups that will run per storage. Since the number of storages
// is unknown at initialisation, workers are created lazily as new storage
// names are encountered.
//
// Note: When both `parallel` and `parallelStorage` are zero or less no workers
// are created and the pipeline will block forever.
func NewParallelPipeline(next Pipeline, parallel, parallelStorage int) *ParallelPipeline {
	var workerSlots chan struct{}
	if parallel > 0 && parallelStorage > 0 {
		// workerSlots allows the total number of parallel jobs to be
		// limited. This allows us to create the required workers for
		// each storage, while still limiting the absolute parallelism.
		workerSlots = make(chan struct{}, parallel)
	}
	return &ParallelPipeline{
		next:            next,
		parallel:        parallel,
		parallelStorage: parallelStorage,
		workerSlots:     workerSlots,
		done:            make(chan struct{}),
		requests:        make(map[string]chan *contextCommand),
	}
}

// Handle queues a request to create a backup. Commands are processed by
// n-workers per storage.
func (p *ParallelPipeline) Handle(ctx context.Context, cmd Command) {
	ch := p.getStorage(cmd.Repository().StorageName)

	select {
	case <-ctx.Done():
		p.setErr(ctx.Err())
	case ch <- &contextCommand{
		Command: cmd,
		Context: ctx,
	}:
	}
}

// Done waits for any in progress calls to `next` to complete then reports any
// accumulated errors
func (p *ParallelPipeline) Done() error {
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
func (p *ParallelPipeline) getStorage(storage string) chan<- *contextCommand {
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
		ch = make(chan *contextCommand)
		p.requests[storage] = ch

		for i := 0; i < workers; i++ {
			p.wg.Add(1)
			go p.worker(ch)
		}
	}
	return ch
}

func (p *ParallelPipeline) worker(ch <-chan *contextCommand) {
	defer p.wg.Done()
	for {
		select {
		case <-p.done:
			return
		case cmd := <-ch:
			p.processCommand(cmd.Context, cmd.Command)
		}
	}
}

func (p *ParallelPipeline) processCommand(ctx context.Context, cmd Command) {
	p.acquireWorkerSlot()
	defer p.releaseWorkerSlot()

	p.next.Handle(ctx, cmd)
}

func (p *ParallelPipeline) setErr(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return
	}
	p.err = err
}

// acquireWorkerSlot queues the worker until a slot is available.
// It never blocks if `parallel` or `parallelStorage` are 0
func (p *ParallelPipeline) acquireWorkerSlot() {
	if p.workerSlots == nil {
		return
	}
	p.workerSlots <- struct{}{}
}

// releaseWorkerSlot releases the worker slot.
func (p *ParallelPipeline) releaseWorkerSlot() {
	if p.workerSlots == nil {
		return
	}
	<-p.workerSlots
}
