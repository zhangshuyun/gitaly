package backup

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

// Strategy used to create/restore backups
type Strategy interface {
	Create(context.Context, *CreateRequest) error
	Restore(context.Context, *RestoreRequest) error
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
