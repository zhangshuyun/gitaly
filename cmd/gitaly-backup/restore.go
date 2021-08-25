package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"

	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backup"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

type restoreRequest struct {
	storage.ServerInfo
	StorageName   string `json:"storage_name"`
	RelativePath  string `json:"relative_path"`
	GlProjectPath string `json:"gl_project_path"`
	AlwaysCreate  bool   `json:"always_create"`
}

type restoreSubcommand struct {
	backupPath string
	locator    string
}

func (cmd *restoreSubcommand) Flags(fs *flag.FlagSet) {
	fs.StringVar(&cmd.backupPath, "path", "", "repository backup path")
	fs.StringVar(&cmd.locator, "locator", "legacy", "determines how backup files are located. One of legacy, pointer. Note: The feature is not ready for production use.")
}

func (cmd *restoreSubcommand) Run(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	sink, err := backup.ResolveSink(ctx, cmd.backupPath)
	if err != nil {
		return fmt.Errorf("restore: resolve sink: %w", err)
	}

	locator, err := backup.ResolveLocator(cmd.locator, sink)
	if err != nil {
		return fmt.Errorf("restore: resolve locator: %w", err)
	}

	manager := backup.NewManager(sink, locator)
	pipeline := backup.NewLoggingPipeline(log.StandardLogger())

	decoder := json.NewDecoder(stdin)
	for {
		var req restoreRequest
		if err := decoder.Decode(&req); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return fmt.Errorf("restore: %w", err)
		}

		repo := gitalypb.Repository{
			StorageName:   req.StorageName,
			RelativePath:  req.RelativePath,
			GlProjectPath: req.GlProjectPath,
		}
		pipeline.Handle(ctx, backup.NewRestoreCommand(manager, req.ServerInfo, &repo, req.AlwaysCreate))
	}

	if err := pipeline.Done(); err != nil {
		return fmt.Errorf("restore: %w", err)
	}
	return nil
}
