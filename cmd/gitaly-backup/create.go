package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"

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

func (cmd *createSubcommand) Flags() *flag.FlagSet {
	fs := flag.NewFlagSet("create", flag.ExitOnError)
	fs.StringVar(&cmd.backupPath, "path", "", "repository backup path")
	return fs
}

func (cmd *createSubcommand) Run(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	fsBackup := backup.NewFilesystem(cmd.backupPath)

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
		if err := fsBackup.BackupRepository(ctx, sr.ServerInfo, &repo); err != nil {
			return err
		}
	}

	return nil
}
