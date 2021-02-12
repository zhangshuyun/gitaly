package repository

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

const (
	MidxRelPath = "objects/pack/multi-pack-index"
)

func (s *server) MidxRepack(ctx context.Context, in *gitalypb.MidxRepackRequest) (*gitalypb.MidxRepackResponse, error) {
	repo := in.GetRepository()

	if err := s.midxSetConfig(ctx, repo); err != nil {
		return nil, err
	}

	for _, cmd := range []midxSubCommand{s.midxWrite, s.midxExpire, s.midxRepack} {
		if err := s.safeMidxCommand(ctx, repo, cmd); err != nil {
			if git.IsInvalidArgErr(err) {
				return nil, helper.ErrInvalidArgumentf("MidxRepack: %w", err)
			}

			return nil, helper.ErrInternal(fmt.Errorf("...%v", err))
		}
	}

	stats.LogObjectsInfo(ctx, s.gitCmdFactory, repo)

	return &gitalypb.MidxRepackResponse{}, nil
}

// midxSubCommand is a helper type to group the helper functions in multi-pack-index
type midxSubCommand func(ctx context.Context, repo repository.GitRepo) error

func (s *server) safeMidxCommand(ctx context.Context, repo repository.GitRepo, cmd midxSubCommand) error {
	if err := cmd(ctx, repo); err != nil {
		return err
	}

	return s.midxEnsureExists(ctx, repo)
}

func (s *server) midxSetConfig(ctx context.Context, repo repository.GitRepo) error {
	cmd, err := s.gitCmdFactory.New(ctx, repo, nil, git.SubCmd{
		Name: "config",
		Flags: []git.Option{
			git.ConfigPair{
				Key:   "core.multiPackIndex",
				Value: "true",
			},
		},
	})
	if err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		return err
	}

	return nil
}

func (s *server) midxWrite(ctx context.Context, repo repository.GitRepo) error {
	cmd, err := s.gitCmdFactory.New(ctx, repo, nil,
		git.SubSubCmd{
			Name:   "multi-pack-index",
			Action: "write",
		},
	)

	if err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		return err
	}

	return nil
}

func (s *server) midxEnsureExists(ctx context.Context, repo repository.GitRepo) error {
	ctxlogger := ctxlogrus.Extract(ctx)

	if err := s.midxVerify(ctx, repo); err != nil {
		ctxlogger.
			WithError(err).
			WithFields(log.Fields{"verify_success": false}).
			Error("MidxRepack")

		return s.midxRewrite(ctx, repo)
	}

	return nil
}

func (s *server) midxVerify(ctx context.Context, repo repository.GitRepo) error {
	ctxlogger := ctxlogrus.Extract(ctx)

	cmd, err := s.gitCmdFactory.New(ctx, repo, nil,
		git.SubSubCmd{
			Name:   "multi-pack-index",
			Action: "verify",
		},
	)
	if err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		return err
	}
	ctxlogger.WithFields(log.Fields{
		"verify_success": true,
	}).Debug("MidxRepack")

	return nil
}

func (s *server) midxRewrite(ctx context.Context, repo repository.GitRepo) error {
	repoPath, err := s.locator.GetRepoPath(repo)
	if err != nil {
		return err
	}

	midxPath := filepath.Join(repoPath, MidxRelPath)

	if err := os.Remove(midxPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	return s.midxWrite(ctx, repo)
}

func (s *server) midxExpire(ctx context.Context, repo repository.GitRepo) error {
	cmd, err := s.gitCmdFactory.New(ctx, repo, nil,
		git.SubSubCmd{
			Name:   "multi-pack-index",
			Action: "expire",
		},
	)
	if err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		return err
	}

	return nil
}

func (s *server) midxRepack(ctx context.Context, repo repository.GitRepo) error {
	repoPath, err := s.locator.GetRepoPath(repo)
	if err != nil {
		return err
	}

	batchSize, err := calculateBatchSize(repoPath)
	if err != nil {
		return err
	}

	// Do not execute a full repack with midxRepack
	// until `git multi-pack-index repack` added support
	// for bitmapindex.
	if batchSize == 0 {
		return nil
	}

	// Note that repack configs:
	//   - repack.useDeltaBaseOffset
	//   - repack.packKeptObjects
	//   - repack.useDeltaIslands
	// will only be respected if git version is >=2.28.0.
	// Bitmap index 'repack.writeBitmaps' is not yet supported.
	cmd, err := s.gitCmdFactory.New(ctx, repo,
		repackConfig(ctx, false),
		git.SubSubCmd{
			Name:   "multi-pack-index",
			Action: "repack",
			Flags: []git.Option{
				git.ValueFlag{Name: "--batch-size", Value: strconv.FormatInt(batchSize, 10)},
			},
		},
	)
	if err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		return err
	}

	return nil
}

// calculateBatchSize returns a batch size that is 1 greater than
// the size of the second largest packfile. This ensures that we will
// repack at least two packs if there are three or more packs.
//
// In case there are less than or equal to 2 packfiles, return 0 for
// a full repack.
//
// Reference:
// - https://public-inbox.org/git/f3b25a9927fe560b764850ea880a71932ec2af32.1598380599.git.gitgitgadget@gmail.com/
func calculateBatchSize(repoPath string) (int64, error) {
	files, err := stats.GetPackfiles(repoPath)
	if err != nil {
		return 0, err
	}

	// In case of 2 or less packs,
	// batch size should be 0 for a full repack
	if len(files) <= 2 {
		return 0, nil
	}

	var biggestSize int64
	var secondBiggestSize int64
	for _, f := range files {
		if f.Size() > biggestSize {
			secondBiggestSize = biggestSize
			biggestSize = f.Size()
			continue
		}

		if f.Size() > secondBiggestSize {
			secondBiggestSize = f.Size()
		}
	}

	// Add 1 so that we always attempt to create a new
	// second biggest pack file
	return secondBiggestSize + 1, nil
}
