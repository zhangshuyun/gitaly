package repository

import (
	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (server) RepackFull(ctx context.Context, in *gitalypb.RepackFullRequest) (*gitalypb.RepackFullResponse, error) {
	if err := repackCommand(ctx, "RepackFull", in.GetRepository(), in.GetCreateBitmap(), "-A", "--pack-kept-objects", "-l"); err != nil {
		return nil, err
	}
	return &gitalypb.RepackFullResponse{}, nil
}

func (server) RepackIncremental(ctx context.Context, in *gitalypb.RepackIncrementalRequest) (*gitalypb.RepackIncrementalResponse, error) {
	if err := repackCommand(ctx, "RepackIncremental", in.GetRepository(), false); err != nil {
		return nil, err
	}
	return &gitalypb.RepackIncrementalResponse{}, nil
}

func repackCommand(ctx context.Context, rpcName string, repo *gitalypb.Repository, bitmap bool, args ...string) error {
	grpc_logrus.Extract(ctx).WithFields(log.Fields{
		"WriteBitmaps": bitmap,
	}).Debug(rpcName)

	var cmdArgs []string
	if bitmap {
		cmdArgs = []string{"-c", "repack.writeBitmaps=true", "repack", "-d"}
	} else {
		cmdArgs = []string{"-c", "repack.writeBitmaps=false", "repack", "-d"}
	}
	cmdArgs = append(cmdArgs, args...)

	cmd, err := git.Command(ctx, repo, cmdArgs...)
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return err
		}
		return status.Errorf(codes.Internal, err.Error())
	}

	if err := cmd.Wait(); err != nil {
		return status.Errorf(codes.Internal, err.Error())
	}

	return nil
}
