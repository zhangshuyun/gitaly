package repository

import (
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git"
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
