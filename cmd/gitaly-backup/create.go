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

type serverRepository struct {
	storage.ServerInfo
	StorageName   string `json:"storage_name"`
	RelativePath  string `json:"relative_path"`
	GlProjectPath string `json:"gl_project_path"`
}

type createSubcommand struct {
	backupPath string
}

func (cmd *createSubcommand) Flags(fs *flag.FlagSet) {
	fs.StringVar(&cmd.backupPath, "path", "", "repository backup path")
}

func (cmd *createSubcommand) Run(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	fsBackup := backup.NewFilesystem(cmd.backupPath)

	var failed int
	decoder := json.NewDecoder(stdin)
	for {
		var sr serverRepository
		if err := decoder.Decode(&sr); err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("create: %w", err)
		}
		repoLog := log.WithFields(log.Fields{
			"storage_name":    sr.StorageName,
			"relative_path":   sr.RelativePath,
			"gl_project_path": sr.GlProjectPath,
		})
		repo := gitalypb.Repository{
			StorageName:   sr.StorageName,
			RelativePath:  sr.RelativePath,
			GlProjectPath: sr.GlProjectPath,
		}
		repoLog.Info("started backup")
		if err := fsBackup.BackupRepository(ctx, sr.ServerInfo, &repo); err != nil {
			if errors.Is(err, backup.ErrSkipped) {
				repoLog.Warn("skipped backup")
			} else {
				repoLog.WithError(err).Error("backup failed")
				failed++
			}
			continue
		}

		repoLog.Info("completed backup")
	}

	if failed > 0 {
		return fmt.Errorf("create: %d failures encountered", failed)
	}
	return nil
}
