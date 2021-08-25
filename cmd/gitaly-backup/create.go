package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"runtime"

	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

type serverRepository struct {
	storage.ServerInfo
	StorageName   string `json:"storage_name"`
	RelativePath  string `json:"relative_path"`
	GlProjectPath string `json:"gl_project_path"`
}

type createSubcommand struct {
	backupPath      string
	parallel        int
	parallelStorage int
	locator         string
}

func (cmd *createSubcommand) Flags(fs *flag.FlagSet) {
	fs.StringVar(&cmd.backupPath, "path", "", "repository backup path")
	fs.IntVar(&cmd.parallel, "parallel", runtime.NumCPU(), "maximum number of parallel backups")
	fs.IntVar(&cmd.parallelStorage, "parallel-storage", 2, "maximum number of parallel backups per storage. Note: actual parallelism when combined with `-parallel` depends on the order the repositories are received.")
	fs.StringVar(&cmd.locator, "locator", "legacy", "determines how backup files are located. One of legacy, pointer. Note: The feature is not ready for production use.")
}

func (cmd *createSubcommand) Run(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	sink, err := backup.ResolveSink(ctx, cmd.backupPath)
	if err != nil {
		return fmt.Errorf("create: resolve sink: %w", err)
	}

	locator, err := backup.ResolveLocator(cmd.locator, sink)
	if err != nil {
		return fmt.Errorf("create: resolve locator: %w", err)
	}

	manager := backup.NewManager(sink, locator)

	var pipeline backup.Pipeline
	pipeline = backup.NewLoggingPipeline(log.StandardLogger())
	if cmd.parallel > 0 || cmd.parallelStorage > 0 {
		pipeline = backup.NewParallelCreatePipeline(pipeline, cmd.parallel, cmd.parallelStorage)
	}

	decoder := json.NewDecoder(stdin)
	for {
		var sr serverRepository
		if err := decoder.Decode(&sr); err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("create: %w", err)
		}
		repo := gitalypb.Repository{
			StorageName:   sr.StorageName,
			RelativePath:  sr.RelativePath,
			GlProjectPath: sr.GlProjectPath,
		}
		pipeline.Handle(ctx, backup.NewCreateCommand(manager, sr.ServerInfo, &repo))
	}

	if err := pipeline.Done(); err != nil {
		return fmt.Errorf("create: %w", err)
	}
	return nil
}
