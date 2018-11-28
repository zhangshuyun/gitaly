package repository

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/housekeeping"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (server) GarbageCollect(ctx context.Context, in *gitalypb.GarbageCollectRequest) (*gitalypb.GarbageCollectResponse, error) {
	ctxlogger := grpc_logrus.Extract(ctx)
	ctxlogger.WithFields(log.Fields{
		"WriteBitmaps": in.GetCreateBitmap(),
	}).Debug("GarbageCollect")

	repoPath, err := helper.GetRepoPath(in.GetRepository())
	if err != nil {
		return nil, err
	}

	if err := cleanupRepo(repoPath); err != nil {
		return nil, err
	}

	if err := cleanupKeepArounds(ctx, in.GetRepository()); err != nil {
		return nil, err
	}

	args := []string{"-c"}
	if in.GetCreateBitmap() {
		args = append(args, "repack.writeBitmaps=true")
	} else {
		args = append(args, "repack.writeBitmaps=false")
	}
	args = append(args, "gc")
	cmd, err := git.Command(ctx, in.GetRepository(), args...)
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, status.Errorf(codes.Internal, "GarbageCollect: gitCommand: %v", err)
	}

	if err := cmd.Wait(); err != nil {
		return nil, status.Errorf(codes.Internal, "GarbageCollect: cmd wait: %v", err)
	}

	// Perform housekeeping post GC
	err = housekeeping.Perform(ctx, repoPath)
	if err != nil {
		ctxlogger.WithError(err).Warn("Post gc housekeeping failed")
	}

	return &gitalypb.GarbageCollectResponse{}, nil
}

func cleanupKeepArounds(ctx context.Context, repo *gitalypb.Repository) error {
	repoPath, err := helper.GetRepoPath(repo)
	if err != nil {
		return nil
	}

	batch, err := catfile.New(ctx, repo)
	if err != nil {
		return nil
	}

	keepAroundsPrefix := "refs/keep-around"
	keepAroundsPath := filepath.Join(repoPath, keepAroundsPrefix)

	f, err := os.Open(keepAroundsPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	// Passing -1 causes us to get the entire directory. This should be ok
	// since this is only containing loose refs
	refInfos, err := f.Readdir(-1)
	if err != nil {
		return err
	}

	for _, info := range refInfos {
		if info.IsDir() {
			return nil
		}

		refName := fmt.Sprintf("%s/%s", keepAroundsPrefix, info.Name())
		path := filepath.Join(repoPath, keepAroundsPrefix, info.Name())

		if info.Size() == 0 {
			// if the file is empty, we already know we need to fix it
			return fixRef(ctx, repo, batch, path, refName, info.Name())
		}

		if _, err = batch.Info(refName); err != nil {
			return fixRef(ctx, repo, batch, path, refName, info.Name())
		}
	}

	return nil
}

func fixRef(ctx context.Context, repo *gitalypb.Repository, batch *catfile.Batch, refPath string, name string, sha string) error {
	// So the ref is broken, let's get rid of it
	if err := os.RemoveAll(refPath); err != nil {
		return err
	}

	// If the name was a valid sha, recreate the ref
	if _, err := batch.Info(sha); err == nil {
		updateRefArgs := []string{"update-ref", name, sha}
		cmd, err := git.Command(ctx, repo, updateRefArgs...)
		if err != nil {
			return err
		}

		if err = cmd.Wait(); err != nil {
			return err
		}
	}
	return nil
}
