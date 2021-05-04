package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"

	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/backup"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
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
}

func (cmd *restoreSubcommand) Flags(fs *flag.FlagSet) {
	fs.StringVar(&cmd.backupPath, "path", "", "repository backup path")
}

func (cmd *restoreSubcommand) Run(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	fsBackup := backup.NewFilesystem(cmd.backupPath)

	var failed int
	decoder := json.NewDecoder(stdin)
	for {
		var req restoreRequest
		if err := decoder.Decode(&req); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return fmt.Errorf("restore: %w", err)
		}

		repoLog := log.WithFields(log.Fields{
			"storage_name":    req.StorageName,
			"relative_path":   req.RelativePath,
			"gl_project_path": req.GlProjectPath,
		})
		repo := gitalypb.Repository{
			StorageName:   req.StorageName,
			RelativePath:  req.RelativePath,
			GlProjectPath: req.GlProjectPath,
		}
		repoLog.Info("started restore")
		if err := fsBackup.RestoreRepository(ctx, req.ServerInfo, &repo, req.AlwaysCreate); err != nil {
			if errors.Is(err, backup.ErrSkipped) {
				repoLog.WithError(err).Warn("skipped restore")
			} else {
				repoLog.WithError(err).Error("restore failed")
				failed++
			}
			continue
		}

		repoLog.Info("completed restore")
	}

	if failed > 0 {
		return fmt.Errorf("restore: %d failures encountered", failed)
	}
	return nil
}
